// Private failure conditions of the tool.
//
// Each condition reaches the application boundary through `ohno::AppError`.
//
// The `#[ohno::error]` macro injects an OhnoCore field containing
// `Arc<dyn Error + Send + Sync>`, which is `!UnwindSafe` because `Arc` requires
// `T: RefUnwindSafe` and trait objects are `!RefUnwindSafe`. These error types
// are immutable after construction — no `&self` method mutates internal state —
// so observing them through a shared reference during unwind is harmless.
// Ref: docs/unwind-safety.md.
//
// Every field a repository, a plan file, or a command line supplies is rendered
// through `Quotable::quoted`, which escapes the newlines and terminal escapes
// that would otherwise let a crafted name forge a second diagnostic line or
// repaint the terminal. The surrounding `'…'` stay for readability; the escaping
// form supplies its own quotes only when the value needs them. A subprocess's
// stderr is the one exception: `git` and `cargo` quote the names they report,
// and escaping their whole diagnostic would fold it onto one unreadable line.
// Ref: docs/implementation.md, "Diagnostics".

use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::PathBuf;

use semver::Version;

use crate::text::Quotable as _;

/// An OS-level failure prevented starting or communicating with a helper process.
#[ohno::error]
#[display("I/O failure while executing `{program}`")]
pub(crate) struct CommandIoError {
    program: String,
}

impl UnwindSafe for CommandIoError {}
impl RefUnwindSafe for CommandIoError {}

/// A helper process exited unsuccessfully.
#[ohno::error]
#[display("`{program}` exited with status {status}: {stderr}")]
pub(crate) struct CommandFailedError {
    program: String,
    status: String,
    stderr: String,
}

impl UnwindSafe for CommandFailedError {}
impl RefUnwindSafe for CommandFailedError {}

impl CommandFailedError {
    pub(crate) fn stderr(&self) -> &str {
        &self.stderr
    }
}

/// Git reported a path that is not valid UTF-8.
///
/// A file name on Unix is an arbitrary byte string, so this is reachable for a
/// tracked file rather than only for a corrupt repository. Decoding such a name
/// lossily would replace the offending bytes, which can collapse two distinct
/// paths into one entry and makes every later `git show` of that path address a
/// different file, so the run stops instead. The name is rendered lossily for
/// the operator's benefit only.
#[ohno::error]
#[display("Git reported a path that is not valid UTF-8: '{}'", path.quoted())]
pub(crate) struct NonUtf8PathError {
    path: String,
}

impl UnwindSafe for NonUtf8PathError {}
impl RefUnwindSafe for NonUtf8PathError {}

/// A blob recorded in history is not valid UTF-8.
///
/// Every blob this tool reads as text is a Cargo manifest, and Cargo requires
/// UTF-8. Replacing invalid bytes could yield a parseable document Git does not
/// store, so the run stops instead.
#[ohno::error]
#[display(
    "Blob '{}:{}' is not valid UTF-8",
    commit.quoted(),
    path.quoted()
)]
pub(crate) struct NonUtf8BlobError {
    commit: String,
    path: String,
}

impl UnwindSafe for NonUtf8BlobError {}
impl RefUnwindSafe for NonUtf8BlobError {}

/// A single path does not fit the platform command-line budget.
#[ohno::error]
#[display(
    "Path '{}' is too long to pass to a 'git' subprocess",
    path.quoted()
)]
pub(crate) struct PathTooLongError {
    path: String,
}

impl UnwindSafe for PathTooLongError {}
impl RefUnwindSafe for PathTooLongError {}

/// A file could not be read.
#[ohno::error]
#[display("Failed to read '{}'", path.quoted())]
pub(crate) struct ReadFileError {
    path: PathBuf,
}

impl UnwindSafe for ReadFileError {}
impl RefUnwindSafe for ReadFileError {}

/// A file could not be written.
#[ohno::error]
#[display("Failed to write '{}'", path.quoted())]
pub(crate) struct WriteFileError {
    path: PathBuf,
}

impl UnwindSafe for WriteFileError {}
impl RefUnwindSafe for WriteFileError {}

/// An expanded-plan output directory could not be created.
#[ohno::error]
#[display("Failed to create output directory '{}'", path.quoted())]
pub(crate) struct CreateOutputDirectoryError {
    path: PathBuf,
}

impl UnwindSafe for CreateOutputDirectoryError {}
impl RefUnwindSafe for CreateOutputDirectoryError {}

/// A TOML document is not valid.
#[ohno::error]
#[display("Failed to parse '{}'", path.quoted())]
pub(crate) struct ParseTomlError {
    path: PathBuf,
}

impl UnwindSafe for ParseTomlError {}
impl RefUnwindSafe for ParseTomlError {}

/// `cargo metadata` JSON is not valid.
#[ohno::error]
#[display("Failed to parse cargo metadata JSON")]
pub(crate) struct ParseMetadataError;

impl UnwindSafe for ParseMetadataError {}
impl RefUnwindSafe for ParseMetadataError {}

/// A plan file is not valid JSON.
#[ohno::error]
#[display("Failed to parse plan '{}'", path.quoted())]
pub(crate) struct ParsePlanError {
    path: PathBuf,
}

impl UnwindSafe for ParsePlanError {}
impl RefUnwindSafe for ParsePlanError {}

/// The plan uses a schema version this tool does not implement.
#[ohno::error]
#[display(
    "Unsupported plan schema_version {version}; regenerate evidence with \
     `cargo release-plan prepare` and the reviewed plan with `cargo release-plan preview`"
)]
pub(crate) struct UnsupportedPlanSchemaError {
    version: u32,
}

impl UnwindSafe for UnsupportedPlanSchemaError {}
impl RefUnwindSafe for UnsupportedPlanSchemaError {}

#[cfg(test)]
impl UnsupportedPlanSchemaError {
    pub(crate) fn version(&self) -> u32 {
        self.version
    }
}

/// An increment entry is missing `level` and `version`, or supplies both.
#[ohno::error]
#[display(
    "Plan increment '{}' must supply exactly one of `level` or `version`",
    name.quoted()
)]
pub(crate) struct PlanIncrementSpecError {
    name: String,
}

impl UnwindSafe for PlanIncrementSpecError {}
impl RefUnwindSafe for PlanIncrementSpecError {}

#[cfg(test)]
impl PlanIncrementSpecError {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// A plan names a package that is not a tracked workspace version target.
#[ohno::error]
#[display("Plan increment '{}' is not a tracked workspace member", name.quoted())]
pub(crate) struct UnknownPlanTargetError {
    name: String,
}

impl UnwindSafe for UnknownPlanTargetError {}
impl RefUnwindSafe for UnknownPlanTargetError {}

#[cfg(test)]
impl UnknownPlanTargetError {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// An expanded plan no longer names every package it reaches.
///
/// An expanded plan lists every package the decision moves, so presentation and
/// application use the same set. Expanding it again must therefore reproduce
/// exactly that set. Reaching a package it does not name means the workspace's
/// derived group changed after the document was produced, so applying it
/// would edit an unlisted package.
#[ohno::error]
#[display(
    "Expanded plan reaches packages it does not name: {}. The workspace's version groups changed \
     after this document was produced, so expand the proposed plan again and review the wider set",
    unnamed.join(", ")
)]
pub(crate) struct ExpandedPlanDriftError {
    unnamed: Vec<String>,
}

impl UnwindSafe for ExpandedPlanDriftError {}
impl RefUnwindSafe for ExpandedPlanDriftError {}

#[cfg(test)]
impl ExpandedPlanDriftError {
    pub(crate) fn unnamed(&self) -> &[String] {
        &self.unnamed
    }
}

/// An expanded plan carries an increment level instead of an explicit version.
///
/// An expanded plan records the version each package will take, which is what
/// makes reviewing one meaningful. A level is resolved against the manifests as
/// they stand when it is applied, so the same expanded document could apply a
/// different version than the one that was reviewed.
#[ohno::error]
#[display(
    "Expanded plan leaves an increment level unresolved for: {}. An expanded plan records the \
     version each package takes, so expand the proposed plan again",
    unresolved.join(", ")
)]
pub(crate) struct UnresolvedExpandedPlanError {
    unresolved: Vec<String>,
}

impl UnwindSafe for UnresolvedExpandedPlanError {}
impl RefUnwindSafe for UnresolvedExpandedPlanError {}

#[cfg(test)]
impl UnresolvedExpandedPlanError {
    pub(crate) fn unresolved(&self) -> &[String] {
        &self.unresolved
    }
}

/// An increment level is not `major`, `minor`, or `patch`.
#[ohno::error]
#[display(
    "Unknown increment level '{}' for '{}'",
    level.quoted(),
    name.quoted()
)]
pub(crate) struct UnknownIncrementLevelError {
    name: String,
    level: String,
}

impl UnwindSafe for UnknownIncrementLevelError {}
impl RefUnwindSafe for UnknownIncrementLevelError {}

/// A version string is not valid `SemVer`.
#[ohno::error]
#[display(
    "Invalid version '{}' for '{}'",
    version.quoted(),
    name.quoted()
)]
pub(crate) struct InvalidVersionError {
    name: String,
    version: String,
}

impl UnwindSafe for InvalidVersionError {}
impl RefUnwindSafe for InvalidVersionError {}

/// History ended before a version change (including creation) was observed.
#[ohno::error]
#[display(
    "Shallow or truncated history: no version change found for package '{}' on the base first-parent line",
    package.quoted()
)]
pub(crate) struct ShallowHistoryError {
    package: String,
}

impl UnwindSafe for ShallowHistoryError {}
impl RefUnwindSafe for ShallowHistoryError {}

#[cfg(test)]
impl ShallowHistoryError {
    pub(crate) fn package(&self) -> &str {
        &self.package
    }
}

/// The base revision could not be resolved.
#[ohno::error]
#[display("Failed to resolve base revision '{}'", rev.quoted())]
pub(crate) struct UnresolvedBaseError {
    rev: String,
}

impl UnwindSafe for UnresolvedBaseError {}
impl RefUnwindSafe for UnresolvedBaseError {}

/// Two increments demand different explicit versions for the same group.
#[ohno::error]
#[display(
    "Conflicting explicit versions for version group '{}'",
    group.quoted()
)]
pub(crate) struct ConflictingPlanVersionError {
    group: String,
}

impl UnwindSafe for ConflictingPlanVersionError {}
impl RefUnwindSafe for ConflictingPlanVersionError {}

/// Entries affecting one target mix an increment level with an explicit version.
#[ohno::error]
#[display(
    "Plan target '{}' mixes increment levels with explicit versions",
    target.quoted()
)]
pub(crate) struct ConflictingPlanIncrementKindError {
    target: String,
}

impl UnwindSafe for ConflictingPlanIncrementKindError {}
impl RefUnwindSafe for ConflictingPlanIncrementKindError {}

/// The current workspace still declares the obsolete manual group key.
#[ohno::error]
#[display(
    "Workspace metadata key 'release-plan.groups' is obsolete; declare version-group membership with exact '=major.minor.patch' workspace dependencies"
)]
pub(crate) struct LegacyVersionGroupsError {}

impl UnwindSafe for LegacyVersionGroupsError {}
impl RefUnwindSafe for LegacyVersionGroupsError {}

/// An in-workspace exact dependency does not use the supported plain triplet.
#[ohno::error]
#[display(
    "Dependency '{}' in {} of '{}' uses unsupported exact requirement '{}'; use one '=major.minor.patch' comparator",
    dependency.quoted(),
    location.quoted(),
    manifest.quoted(),
    requirement.quoted()
)]
pub(crate) struct UnsupportedExactRequirementError {
    manifest: String,
    dependency: String,
    location: String,
    requirement: String,
}

impl UnwindSafe for UnsupportedExactRequirementError {}
impl RefUnwindSafe for UnsupportedExactRequirementError {}

/// A group is assigned an explicit version that exact pins cannot represent.
#[ohno::error]
#[display(
    "Version group '{}' cannot use non-plain target version '{}'; use a major.minor.patch version",
    group.quoted(),
    version
)]
pub(crate) struct NonPlainGroupVersionError {
    group: String,
    version: Version,
}

impl UnwindSafe for NonPlainGroupVersionError {}
impl RefUnwindSafe for NonPlainGroupVersionError {}

/// Incrementing a semantic-version component overflows `u64`.
#[ohno::error]
#[display("Incrementing version '{version}' overflows a SemVer component")]
pub(crate) struct VersionOverflowError {
    version: Version,
}

impl UnwindSafe for VersionOverflowError {}
impl RefUnwindSafe for VersionOverflowError {}

#[cfg(test)]
impl VersionOverflowError {
    pub(crate) fn version(&self) -> &Version {
        &self.version
    }
}

/// A declared version is lower than the version already released at the anchor.
#[ohno::error]
#[display(
    "Package '{}' declares version {declared}, which is lower than {anchor_version} \
     released at anchor {anchor_commit}",
    package.quoted()
)]
pub(crate) struct VersionRegressionError {
    package: String,
    declared: Version,
    anchor_version: Version,
    anchor_commit: String,
}

impl UnwindSafe for VersionRegressionError {}
impl RefUnwindSafe for VersionRegressionError {}

#[cfg(test)]
impl VersionRegressionError {
    pub(crate) fn package(&self) -> &str {
        &self.package
    }
}

/// A plan asks for a version lower than one a target already declares.
#[ohno::error]
#[display(
    "Plan sets '{}' to {requested}, which is lower than the declared {declared}",
    target.quoted()
)]
pub(crate) struct PlanVersionRegressionError {
    target: String,
    requested: Version,
    declared: Version,
}

impl UnwindSafe for PlanVersionRegressionError {}
impl RefUnwindSafe for PlanVersionRegressionError {}

#[cfg(test)]
impl PlanVersionRegressionError {
    pub(crate) fn target(&self) -> &str {
        &self.target
    }
}

/// An `include` / `exclude` pattern is not a valid gitignore rule.
#[ohno::error]
#[display("Invalid packaging pattern '{}'", pattern.quoted())]
pub(crate) struct InvalidPackagingPatternError {
    pattern: String,
}

impl UnwindSafe for InvalidPackagingPatternError {}
impl RefUnwindSafe for InvalidPackagingPatternError {}

#[cfg(test)]
impl InvalidPackagingPatternError {
    pub(crate) fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// A `[workspace] members` / `exclude` entry is not a valid path pattern.
#[ohno::error]
#[display("Invalid workspace member pattern '{}'", pattern.quoted())]
pub(crate) struct InvalidMemberPatternError {
    pattern: String,
}

impl UnwindSafe for InvalidMemberPatternError {}
impl RefUnwindSafe for InvalidMemberPatternError {}

#[cfg(test)]
impl InvalidMemberPatternError {
    pub(crate) fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// Released content contains a symbolic link.
///
/// Cargo dereferences a link when it builds a package archive, so the released bytes are
/// the target's content, while Git stores the link as a blob holding the target
/// path. Comparing the blobs would call a package unchanged after an edit to the
/// file it points at, and reconstructing the target's historical content is only
/// possible when the link stays inside the repository at both ends. A refusal is
/// preferred over a release verdict that can be silently wrong. Ref:
/// docs/design.md, "Released content".
#[ohno::error]
#[display(
    "Package '{}' releases '{}', which is a symbolic link",
    package.quoted(),
    path.quoted()
)]
pub(crate) struct SymlinkReleasedError {
    package: String,
    path: String,
}

impl UnwindSafe for SymlinkReleasedError {}
impl RefUnwindSafe for SymlinkReleasedError {}

/// A Cargo lockfile does not describe a resolved dependency graph.
///
/// The lockfile is machine-written, so a lockfile that does not parse means the
/// tool is reading something other than what it believes. Guessing a closure
/// from it would silently under-report the dependencies a packaged target ships.
#[ohno::error]
#[display("Failed to read the resolved dependencies in '{}'", path.quoted())]
pub(crate) struct MalformedLockfileError {
    path: String,
}

impl UnwindSafe for MalformedLockfileError {}
impl RefUnwindSafe for MalformedLockfileError {}

/// A package's published dependency closure cannot be reconstructed.
///
/// Classification requires a workspace lockfile at every comparison endpoint
/// where the package has an installable binary target.
#[ohno::error]
#[display(
    "Cannot assess locked dependencies for package '{}' with an installable binary target: {reason}",
    package.quoted()
)]
pub(crate) struct LockfileClosureUnavailableError {
    package: String,
    reason: String,
}

impl UnwindSafe for LockfileClosureUnavailableError {}
impl RefUnwindSafe for LockfileClosureUnavailableError {}

/// A package's private-API declaration is present but is not a boolean.
///
/// Fails closed rather than defaulting: a typo here would otherwise silently
/// decide whether the package is assessed for API compatibility at all.
#[ohno::error]
#[display(
    "Package '{}' declares `[package.metadata.release-plan] private-api` as {}, which must be a boolean",
    package.quoted(),
    value.quoted()
)]
pub(crate) struct MalformedPrivateApiError {
    package: String,
    value: String,
}

impl UnwindSafe for MalformedPrivateApiError {}
impl RefUnwindSafe for MalformedPrivateApiError {}

#[cfg(test)]
impl MalformedPrivateApiError {
    pub(crate) fn package(&self) -> &str {
        &self.package
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fmt::Debug;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::path::Path;
    use std::{error, io};

    use ohno::ErrorExt as _;
    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(
        CommandIoError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        CommandFailedError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        NonUtf8PathError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ReadFileError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        WriteFileError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ParseTomlError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ParseMetadataError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ParsePlanError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnsupportedPlanSchemaError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        PlanIncrementSpecError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnknownPlanTargetError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnknownIncrementLevelError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        InvalidVersionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ShallowHistoryError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnresolvedBaseError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ConflictingPlanVersionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ConflictingPlanIncrementKindError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        LegacyVersionGroupsError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnsupportedExactRequirementError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        NonPlainGroupVersionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        VersionOverflowError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        InvalidPackagingPatternError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        InvalidMemberPatternError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        SymlinkReleasedError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        NonUtf8BlobError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        PathTooLongError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        MalformedLockfileError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        LockfileClosureUnavailableError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        CreateOutputDirectoryError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        ExpandedPlanDriftError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnresolvedExpandedPlanError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );

    assert_impl_all!(
        VersionRegressionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        PlanVersionRegressionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    #[test]
    fn command_io_error_retains_start_write_and_wait_causes() {
        for (kind, cause) in [
            (io::ErrorKind::NotFound, "process creation"),
            (io::ErrorKind::BrokenPipe, "stdin write"),
            (io::ErrorKind::Other, "process wait"),
        ] {
            let error = CommandIoError::caused_by("git", io::Error::new(kind, cause));
            let source = error.find_source::<io::Error>().unwrap();
            assert_eq!(source.kind(), kind);
            assert_eq!(source.to_string(), cause);
        }
    }

    #[test]
    fn shallow_history_error_names_package() {
        let error = ShallowHistoryError::new("nm");
        assert_eq!(error.package(), "nm");
    }

    #[test]
    fn unsupported_plan_schema_error_carries_version() {
        // An arbitrary unsupported schema revision; the test covers round-tripping
        // through the error, not revision-compatibility semantics.
        let error = UnsupportedPlanSchemaError::new(9_u32);
        assert_eq!(error.version(), 9);
    }

    #[test]
    fn plan_increment_spec_error_names_target() {
        let error = PlanIncrementSpecError::new("nm");
        assert_eq!(error.name(), "nm");
    }

    #[test]
    fn unknown_plan_target_error_names_target() {
        let error = UnknownPlanTargetError::new("ghost");
        assert_eq!(error.name(), "ghost");
    }

    #[test]
    fn version_overflow_error_carries_version() {
        // An arbitrary ordinary semantic version; the test covers retained
        // context, not the overflow arithmetic that produces the error.
        let version: Version = "1.2.3".parse().unwrap();
        let error = VersionOverflowError::new(version.clone());
        assert_eq!(error.version(), &version);
    }

    #[test]
    fn version_regression_error_names_package() {
        // Arbitrary ordering-valid versions; the test covers retained context.
        let declared: Version = "0.1.0".parse().unwrap();
        let anchor: Version = "0.2.0".parse().unwrap();
        let error = VersionRegressionError::new("nm", declared, anchor, "abc123");
        assert_eq!(error.package(), "nm");
    }

    /// A repository controlled value is escaped in the message.
    ///
    /// A manifest can name a package or a pattern with a newline in it, and the message would
    /// otherwise carry that newline into a log where the tail reads as a fresh line of the tool's
    /// own output.
    ///
    /// The condition renders on the first line, followed by its cause and, when
    /// backtraces are enabled, a captured backtrace. Escaping is therefore
    /// asserted on that first line: an unescaped value would push the tail of
    /// the pattern off it.
    #[test]
    fn a_repository_controlled_value_is_escaped_in_the_message() {
        let error = InvalidMemberPatternError::new("a\nb");
        let message = error.to_string();
        let first_line = message.lines().next().unwrap();
        assert!(first_line.contains(r"a\nb"), "{message}");
    }

    /// The same protection applies to the paths the file-access errors name.
    #[test]
    fn a_repository_controlled_path_is_escaped_in_the_message() {
        let error = ReadFileError::caused_by(Path::new("a\nb"), io::Error::other("x"));
        let message = error.to_string();
        let first_line = message.lines().next().unwrap();
        assert!(first_line.contains(r"a\nb"), "{message}");
    }

    /// An ordinary value gains no escaping, so the common message stays plain.
    #[test]
    fn an_ordinary_value_is_left_alone_in_the_message() {
        let error = InvalidMemberPatternError::new("packages/*");
        assert!(error.to_string().contains("'packages/*'"));
    }
}
