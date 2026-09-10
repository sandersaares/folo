// Released-content matching.
//
// A package's released files are git-tracked paths under its directory, filtered
// by the manifest `include` / `exclude` using gitignore-style matching (the
// `ignore` crate). The package's own `Cargo.lock` is never released content: the
// published lockfile is derived per package when the archive is built and is not a function of
// the package source. A lockfile nested deeper in the package is ordinary source.
//
// The files named by `readme` and `license-file` are released content wherever
// they live, because Cargo copies each into the crate root regardless of
// `include` and `exclude`. Those are resolved in `classify` rather than here,
// since they are located by manifest key rather than by pattern.

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use ohno::AppError;

use crate::InvalidPackagingPatternError;

/// Include / exclude rules from a package manifest.
///
/// Matchers are compiled once when the rules are parsed so classification can
/// query many paths without rebuilding gitignore state per file.
#[derive(Clone, Debug, Default)]
pub(crate) struct PackagingRules {
    selection: Selection,
}

/// The build directory Cargo never packs, relative to the package root.
const BUILD_DIR: &str = "target";

impl PackagingRules {
    pub(crate) fn new(
        include: Option<&[String]>,
        exclude: Option<&[String]>,
    ) -> Result<Self, AppError> {
        // Cargo consults `exclude` only when there is no `include`, so the
        // selection is decided once here rather than at every query. Compiling
        // an `exclude` that an `include` overrides would retain a matcher that
        // must never be consulted, and consulting it would drop a path Cargo
        // packs.
        let selection = match (include, exclude) {
            (Some(include), _) => Selection::AllowList(compile_gitignore(include)?),
            (None, Some(exclude)) => Selection::DenyList(compile_gitignore(exclude)?),
            (None, None) => Selection::Everything,
        };
        Ok(Self { selection })
    }

    /// Whether `package_relative_path` would be put in the package archive.
    ///
    /// The path is Git's, so `/` is the separator and every other byte —
    /// including `\` — is part of a file's name.
    ///
    /// `Cargo.toml` is always released. The package's own `Cargo.lock` is never
    /// released; a lockfile in a subdirectory is ordinary package source. The
    /// build directory at the package root is never released either, whatever
    /// the manifest keys say.
    ///
    /// Matching consults each parent directory as well as the path itself, so a
    /// directory pattern such as `src/` covers everything beneath it the way it
    /// does in Cargo and in `.gitignore`.
    pub(crate) fn is_released(&self, package_relative_path: &str) -> bool {
        let path = package_relative_path.trim_start_matches("./");
        if path == "Cargo.lock" {
            return false;
        }
        if path == "Cargo.toml" {
            return true;
        }
        // Ref: docs/design.md, "Where Cargo departs from those rules".
        if path == BUILD_DIR
            || path
                .strip_prefix(BUILD_DIR)
                .is_some_and(|rest| rest.starts_with('/'))
        {
            return false;
        }
        match &self.selection {
            Selection::AllowList(include) => {
                include.matched_path_or_any_parents(path, false).is_ignore()
            }
            Selection::DenyList(exclude) => {
                !exclude.matched_path_or_any_parents(path, false).is_ignore()
            }
            Selection::Everything => true,
        }
    }
}

/// The one file-selection mode a manifest's packaging keys resolve to.
///
/// Cargo's `include` and `exclude` are not independent filters: an `include`
/// list is an allow-list that supersedes `exclude` entirely. Storing the
/// resolved mode rather than both inputs keeps that precedence in one place and
/// leaves no inactive matcher for a later consumer to apply by mistake.
#[derive(Clone, Debug, Default)]
enum Selection {
    /// `include` is present: only what it matches is released.
    AllowList(Gitignore),
    /// Only `exclude` is present: everything it does not match is released.
    DenyList(Gitignore),
    /// Neither key is present: every path under the package is released.
    #[default]
    Everything,
}

fn compile_gitignore(patterns: &[String]) -> Result<Gitignore, AppError> {
    let mut builder = GitignoreBuilder::new("");
    for pattern in patterns {
        builder
            .add_line(None, pattern)
            .map_err(|error| InvalidPackagingPatternError::caused_by(pattern, error))?;
    }
    builder
        .build()
        .map_err(|error| InvalidPackagingPatternError::caused_by("include/exclude", error).into())
}

/// Relative path of `full` inside `package_dir`, both repo-relative with `/`.
pub(crate) fn relativize<'a>(full: &'a str, package_dir: &str) -> Option<&'a str> {
    let full = full.trim_start_matches("./");
    if package_dir.is_empty() || package_dir == "." {
        return Some(full);
    }
    let prefix = package_dir.trim_end_matches('/');
    if full == prefix {
        return None;
    }
    full.strip_prefix(prefix)?.strip_prefix('/')
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::InvalidPackagingPatternError;

    fn rules(
        include: Option<&[&str]>,
        exclude: Option<&[&str]>,
    ) -> Result<PackagingRules, AppError> {
        let include = include.map(|patterns| {
            patterns
                .iter()
                .map(|pattern| (*pattern).to_string())
                .collect::<Vec<_>>()
        });
        let exclude = exclude.map(|patterns| {
            patterns
                .iter()
                .map(|pattern| (*pattern).to_string())
                .collect::<Vec<_>>()
        });
        PackagingRules::new(include.as_deref(), exclude.as_deref())
    }

    /// A backslash is part of a file name.
    ///
    /// Git reports `/`-separated paths on every platform, so a `\` in one is a character of a
    /// file's name and must not be read as a directory boundary. The leading `./` Cargo tolerates
    /// in its own listings still comes off.
    #[test]
    fn a_backslash_is_part_of_a_file_name() {
        let rules = rules(Some(&["/src/"]), None).unwrap();
        assert!(rules.is_released("./src/lib.rs"));
        assert!(rules.is_released(r"src/odd\name.rs"));
        assert!(!rules.is_released(r"benches\bench.rs"));
    }

    /// A package directory has no relative path inside itself.
    ///
    /// The package directory itself is not a path inside the package, so it has no relative form
    /// and must not be mistaken for the package root file.
    #[test]
    fn a_package_directory_has_no_relative_path_inside_itself() {
        assert_eq!(relativize("packages/foo", "packages/foo"), None);
        assert_eq!(
            relativize("packages/foo/src/lib.rs", "packages/foo"),
            Some("src/lib.rs")
        );
        assert_eq!(
            relativize("packages/foo/src/lib.rs", ""),
            Some("packages/foo/src/lib.rs")
        );
        assert_eq!(relativize("other/foo.rs", "packages/foo"), None);
    }

    #[test]
    fn cargo_toml_is_always_released() {
        let rules = rules(Some(&["/src/"]), None).unwrap();
        assert!(rules.is_released("Cargo.toml"));
    }

    #[test]
    fn only_the_package_lockfile_is_never_released() {
        let rules = PackagingRules::default();
        assert!(!rules.is_released("Cargo.lock"));
        // A lockfile below the package root belongs to something the package
        // ships, such as a test fixture workspace, so it is ordinary source.
        assert!(rules.is_released("fixtures/Cargo.lock"));
    }

    #[test]
    fn the_build_directory_is_never_released_by_default() {
        // Cargo drops it before reading either manifest key, so neither the
        // default selection nor an `include` naming it can pack build output.
        let default = PackagingRules::default();
        assert!(!default.is_released("target"));
        assert!(!default.is_released("target/debug/demo"));

        // Only the directory itself is special; a name that merely starts with
        // the same letters is ordinary source.
        assert!(default.is_released("targets.rs"));
        assert!(default.is_released("src/target/mod.rs"));
    }

    #[test]
    fn the_build_directory_is_never_released_by_include() {
        let included = rules(Some(&["/target/", "/src/"]), None).unwrap();
        assert!(!included.is_released("target/debug/demo"));
        assert!(included.is_released("src/lib.rs"));
    }

    #[test]
    fn the_build_directory_is_never_released_by_exclude() {
        let excluded = rules(None, Some(&["/tests/"])).unwrap();
        assert!(!excluded.is_released("target/debug/demo"));
    }

    #[test]
    fn include_allow_list_keeps_matching_paths() {
        let rules = rules(Some(&["/src/", "/README.md"]), None).unwrap();
        assert!(rules.is_released("src/lib.rs"));
        assert!(rules.is_released("README.md"));
        assert!(!rules.is_released("tests/foo.rs"));
        assert!(!rules.is_released("benches/foo.rs"));
    }

    #[test]
    fn include_later_negation_drops_a_subset() {
        let rules = rules(Some(&["/src/", "!/src/private/"]), None).unwrap();
        assert!(rules.is_released("src/lib.rs"));
        assert!(!rules.is_released("src/private/x.rs"));
    }

    #[test]
    fn exclude_drops_matching_paths_when_include_absent() {
        let rules = rules(None, Some(&["/tests/"])).unwrap();
        assert!(rules.is_released("src/lib.rs"));
        assert!(!rules.is_released("tests/foo.rs"));
    }

    #[test]
    fn include_ignores_exclude() {
        let rules = rules(Some(&["/tests/"]), Some(&["/tests/"])).unwrap();
        assert!(rules.is_released("tests/foo.rs"));
    }

    #[test]
    fn recursive_glob_includes_nested_files() {
        // Most rule tests need only literal directory matching. Keep recursive-glob coverage
        // on a short prefix so regex compilation does not dominate the Miri workload.
        let rules = rules(Some(&["s/**"]), None).unwrap();
        assert!(rules.is_released("s/a/b"));
        assert!(!rules.is_released("t/a"));
    }

    /// An invalid include pattern is an error.
    ///
    /// The sample leaves a brace alternation unterminated, which the glob
    /// syntax cannot complete, so it exercises rejection rather than any
    /// particular malformed spelling.
    #[test]
    fn invalid_include_pattern_is_an_error() {
        let error = rules(Some(&["foo.{js,ts"]), None).unwrap_err();
        let source = error
            .find_source::<InvalidPackagingPatternError>()
            .expect("invalid packaging pattern");
        assert_eq!(source.pattern(), "foo.{js,ts");
    }

    #[test]
    fn no_rules_releases_everything_but_lockfile() {
        let rules = PackagingRules::default();
        assert!(rules.is_released("src/lib.rs"));
        assert!(rules.is_released("tests/foo.rs"));
        assert!(!rules.is_released("Cargo.lock"));
    }

    #[test]
    fn relativize_strips_package_dir() {
        assert_eq!(
            relativize("packages/foo/src/lib.rs", "packages/foo"),
            Some("src/lib.rs")
        );
        assert_eq!(
            relativize("packages/foo/Cargo.toml", "packages/foo"),
            Some("Cargo.toml")
        );
        assert_eq!(relativize("packages/bar/src/lib.rs", "packages/foo"), None);
        // Empty and `.` are both "workspace root as package dir"; each arm must
        // independently return the full path so `||` cannot become `&&`.
        assert_eq!(relativize("src/lib.rs", ""), Some("src/lib.rs"));
        assert_eq!(relativize("src/lib.rs", "."), Some("src/lib.rs"));
        assert_eq!(relativize("src/lib.rs", "src"), Some("lib.rs"));
    }
}
