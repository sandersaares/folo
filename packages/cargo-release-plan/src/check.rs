// `check` command: fail on a release the workspace's manifests cannot support.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use ohno::AppError;
use semver::Version;

use crate::classify::{
    ChangedItem, Classification, PackageClass, PackageStatus, classify, released_work_tree_paths,
};
use crate::command::run_capture;
use crate::git::os_path;
use crate::groups::GroupVerdict;
use crate::manifest::requirement_names_version;
use crate::metadata::{ExactDependency, VersionTarget};
use crate::verbose::Verbose;
use crate::{quote_path, short_commit};

/// Output format for `cargo release-plan check`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    clippy::exhaustive_enums,
    reason = "Hidden enum for internal/test use only"
)]
pub enum CheckFormat {
    /// Human-readable lines on stderr when the check fails.
    Text,
    /// GitHub Actions workflow annotations in addition to the text lines.
    Github,
}

/// Skill named in check failure text so a failing job is a sufficient prompt.
///
/// The name is the directory under `.github/skills` that an agent loads, so it
/// must track that directory rather than any prose description of the recovery.
const INCREMENT_VERSIONS_SKILL: &str = "increment-versions";

pub(crate) fn run_check(
    base: Option<&str>,
    manifest_path: &Path,
    format: CheckFormat,
    verify_packaging: bool,
    verbose: Verbose,
) -> Result<(bool, String, String), AppError> {
    let classification = classify(manifest_path, base, verbose)?;
    // Every gating defect appends at least one diagnostic line, so the verdict is read back from
    // the rendered diagnostics. Recomputing it from the classification instead would let a rule
    // added to the rendering below be reported without ever failing the check.
    let mut message = render_workspace_diagnostics(
        &classification.packages,
        &classification.groups,
        &classification.base,
        format,
        &classification.work_tree.version_targets,
        &classification.work_tree.exact_dependencies,
    );

    let warnings = if verify_packaging {
        verify_packaging_rules(&classification)
    } else {
        String::new()
    };

    let passed = message.is_empty();

    if let Some(success) = default_success_message(passed, &message) {
        message = success.to_string();
    }

    Ok((passed, message, warnings))
}

fn default_success_message(passed: bool, message: &str) -> Option<&'static str> {
    if passed && message.is_empty() {
        Some("Every release and workspace-version check passed.")
    } else {
        None
    }
}

/// Renders the failing packages and groups as human- or workflow-readable text.
///
/// Takes the classified packages and group verdicts rather than the whole
/// [`Classification`] because the rendering depends on nothing else, and the
/// remainder carries the Git repository and work tree that a caller would
/// otherwise have to build.
fn render_workspace_diagnostics(
    packages: &[PackageClass],
    groups: &BTreeMap<String, GroupVerdict>,
    base: &str,
    format: CheckFormat,
    version_targets: &[VersionTarget],
    exact_dependencies: &[ExactDependency],
) -> String {
    let declared: BTreeMap<&str, &Version> = version_targets
        .iter()
        .map(|target| (target.name.as_str(), &target.version))
        .collect();
    let by_name: BTreeMap<&str, &PackageClass> = packages
        .iter()
        .map(|package| (package.name.as_str(), package))
        .collect();
    let mut lines = Vec::new();
    for package in packages {
        if package.status() != PackageStatus::NeedsIncrement {
            continue;
        }
        let anchor = package.anchor().expect(
            "a package can only fail against an anchor, so a needs-increment verdict always carries the commit it was compared with",
        );
        let anchor = format!("{} ({})", short_commit(&anchor.commit), anchor.version);
        let group_text = match &package.group {
            Some(group) => {
                let members = groups
                    .get(group)
                    .expect("every derived package group has a complete verdict")
                    .members()
                    .join(", ");
                format!(" Group {} also includes {members}.", quote_path(group))
            }
            None => String::new(),
        };
        let changed = match package.changed().first() {
            Some(ChangedItem::Package { path, .. }) => {
                format!("{} (and related paths) changed", quote_path(path))
            }
            Some(ChangedItem::Inherited { field }) => {
                format!("{} (and related paths) changed", quote_path(field))
            }
            Some(ChangedItem::Lockfile { dependency, .. }) => {
                format!(
                    "the locked dependency {} (and related paths) changed",
                    quote_path(dependency)
                )
            }
            None => "released content changed".to_string(),
        };
        let text = format!(
            "{}: needs-increment since {anchor}; {changed}.{group_text} {}",
            quote_path(&package.name),
            remedy(base)
        );
        if format == CheckFormat::Github {
            let file = os_path(&package.manifest_path);
            lines.push(format!(
                "::error file={},title=needs-increment::{}",
                escape_property(&file),
                escape_data(&text)
            ));
        }
        lines.push(text);
    }

    for (name, verdict) in groups {
        if verdict.is_consistent() {
            continue;
        }
        // Naming the version each member declares is what makes the mismatch
        // actionable; a bare member list only restates the group definition.
        let listed = verdict
            .members()
            .iter()
            .map(|member| match declared.get(member.as_str()) {
                Some(version) => Cow::Owned(format!("{}@{version}", quote_path(member))),
                None => quote_path(member),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let text = format!(
            "group {}: members declare different versions ({listed}). {}",
            quote_path(name),
            remedy(base)
        );
        if format == CheckFormat::Github {
            let file = verdict
                .members()
                .first()
                .and_then(|member| version_targets.iter().find(|target| target.name == *member))
                .map(|target| os_path(&target.manifest_path))
                .expect("every derived group member is a version target");
            lines.push(format!(
                "::error file={},title=inconsistent-group::{}",
                escape_property(&file),
                escape_data(&text)
            ));
        }
        lines.push(text);
    }

    for package in packages {
        for dependency in &package.dependencies {
            let Some(dependency_version) = declared.get(dependency.name.as_str()) else {
                continue;
            };
            if requirement_names_version(&dependency.req, dependency_version) {
                continue;
            }
            // The requirement Cargo would publish must name the version the workspace builds and
            // tests against. A wider requirement lets a consumer resolve a combination this
            // workspace never validated, and it makes the release decision depend on requirement
            // arithmetic rather than on the declared versions alone.
            // Ref: docs/dependencies.md, "Intra-workspace requirements name the declared version".
            let expected = if dependency.exact_pin {
                format!("={dependency_version}")
            } else {
                dependency_version.to_string()
            };
            let text = format!(
                "{}: requires {} {}, which does not name the version it declares ({dependency_version}). Change the requirement to {}. {}",
                quote_path(&package.name),
                quote_path(&dependency.name),
                quote_path(&dependency.req),
                quote_path(&expected),
                remedy(base)
            );
            if format == CheckFormat::Github {
                let file = os_path(&package.manifest_path);
                lines.push(format!(
                    "::error file={},title=stale-workspace-requirement::{}",
                    escape_property(&file),
                    escape_data(&text)
                ));
            }
            lines.push(text);
        }
    }

    for dependency in exact_dependencies {
        // Publishable sources already carry the same edge in `ReportedDep` and
        // were checked above. This pass adds declarations from helpers without
        // emitting the same finding twice.
        if by_name.contains_key(dependency.source.as_str()) {
            continue;
        }
        let dependency_version = declared
            .get(dependency.target.as_str())
            .expect("a validated exact dependency targets a tracked version target");
        if requirement_names_version(&dependency.requirement, dependency_version) {
            continue;
        }
        let expected = format!("={dependency_version}");
        let text = format!(
            "{}: requires {} {} in {}, which does not name the version it declares ({dependency_version}). Change the requirement to {}. {}",
            quote_path(&dependency.source),
            quote_path(&dependency.target),
            quote_path(&dependency.requirement),
            quote_path(&dependency.location),
            quote_path(&expected),
            remedy(base)
        );
        if format == CheckFormat::Github {
            let file = os_path(&dependency.manifest_path);
            lines.push(format!(
                "::error file={},title=stale-workspace-requirement::{}",
                escape_property(&file),
                escape_data(&text)
            ));
        }
        lines.push(text);
    }

    for package in packages {
        // A package the baseline has never published has no consumer contract to break, so it
        // cannot owe a breaking release. It also has no anchor to move away from, which means
        // this rule could never be satisfied by changing its version: reporting it would demand
        // an increment that does not exist. It takes the first-publication path instead.
        if package.anchor().is_none() || releases_breaking_change(package) {
            continue;
        }
        for dependency in &package.dependencies {
            if !dependency.public {
                continue;
            }
            let Some(broken) = by_name.get(dependency.name.as_str()) else {
                continue;
            };
            if !releases_breaking_change(broken) {
                continue;
            }
            // A semver-incompatible release changes the identity of the dependency's types for
            // consumers, so a package re-exporting them cannot stay compatible: a consumer
            // holding the older dependency can no longer hand its types to this package. That
            // holds however unrelated the dependency's own breaking change was to the exposed
            // items, so it is decided from the version move rather than from the diff.
            // Ref: docs/design.md, "Public dependencies".
            let anchor = broken.anchor().expect(
                "only a package that releases a breaking change reaches here, which requires an anchor to compare against",
            );
            let text = format!(
                "{}: exposes {} in its public API and must release a breaking change of its own, because {} moves from {} to an incompatible {}. {}",
                quote_path(&package.name),
                quote_path(&dependency.name),
                quote_path(&dependency.name),
                anchor.version,
                broken.declared_version,
                remedy(base)
            );
            if format == CheckFormat::Github {
                let file = os_path(&package.manifest_path);
                lines.push(format!(
                    "::error file={},title=unpropagated-breaking-change::{}",
                    escape_property(&file),
                    escape_data(&text)
                ));
            }
            lines.push(text);
        }
    }

    lines.join("\n")
}

#[cfg(test)]
fn render_diagnostics(
    packages: &[PackageClass],
    groups: &BTreeMap<String, GroupVerdict>,
    base: &str,
    format: CheckFormat,
) -> String {
    let version_targets = packages
        .iter()
        .map(|package| VersionTarget {
            name: package.name.clone(),
            version: package.declared_version.clone(),
            manifest_path: package.manifest_path.clone(),
            publishable: true,
        })
        .collect::<Vec<_>>();
    render_workspace_diagnostics(packages, groups, base, format, &version_targets, &[])
}

/// Whether the package's declared version is a semver-incompatible move from its last release.
///
/// Cargo treats the leftmost non-zero component as the major component, so this
/// is the comparison that decides whether a consumer of the last release
/// resolves the next one.
fn releases_breaking_change(package: &PackageClass) -> bool {
    let Some(anchor) = package.anchor() else {
        // Never released, so there is no consumer contract to break.
        return false;
    };
    compatibility_key(&anchor.version) != compatibility_key(&package.declared_version)
}

/// The components that must agree for two versions to be semver-compatible.
fn compatibility_key(version: &Version) -> (u64, u64, u64) {
    if version.major > 0 {
        return (version.major, 0, 0);
    }
    if version.minor > 0 {
        return (0, version.minor, 0);
    }
    (0, 0, version.patch)
}

/// Escapes the message body of a GitHub workflow command.
///
/// A workflow command ends at the first newline and treats `%` as the escape
/// introducer, so a message carrying either would be truncated or would let
/// repository-controlled text start a second command. The substitutions match
/// GitHub's own toolkit, which is what the runner decodes.
fn escape_data(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

/// Escapes a property value of a GitHub workflow command.
///
/// Property values are additionally delimited by `:` and `,`, so a path
/// containing either would otherwise split into further properties.
fn escape_property(value: &str) -> String {
    escape_data(value).replace(':', "%3A").replace(',', "%2C")
}

/// The remediation sentence appended to every gating diagnostic.
///
/// The self-contained path comes first so the message stays actionable without
/// any tooling beyond this binary; the skill is named as the assisted route.
/// The base is spelled out separately from the copyable command because it can
/// come from a repository-controlled ref name and diagnostic quoting is not
/// shell quoting.
fn remedy(base: &str) -> String {
    format!(
        "Run `cargo release-plan report --out-dir <dir> --base <base>` to inspect the changes, then \
         `cargo release-plan apply --plan <plan.json>` with an increment plan, or run the \
         {INCREMENT_VERSIONS_SKILL} skill to do both. Set `<base>` to the base reported here: {}.",
        quote_path(base)
    )
}

// Cross-checks against `cargo package --list`; not practical to mutate in unit tests.
#[cfg_attr(test, mutants::skip)]
fn verify_packaging_rules(classification: &Classification) -> String {
    let mut warnings = String::new();
    for package in &classification.work_tree.packages {
        let listed = match cargo_package_list(
            &classification.work_tree.workspace_root,
            &package.manifest.name,
        ) {
            Ok(listed) => listed,
            Err(error) => {
                writeln!(
                    warnings,
                    "warning: packaging probe failed for {}: {error}",
                    quote_path(&package.manifest.name)
                )
                .expect("writing to String");
                continue;
            }
        };
        // The released-content selection has to come from classification itself:
        // rebuilding it from `include` and `exclude` would miss a README Cargo
        // detects for itself and take in a nested package's files, warning about a
        // package whose rules are right.
        let tool = match released_work_tree_paths(&classification.git, package, classification.case)
        {
            Ok(paths) => paths,
            Err(error) => {
                writeln!(
                    warnings,
                    "warning: listing released content failed for {}: {error}",
                    quote_path(&package.manifest.name)
                )
                .expect("writing to String");
                continue;
            }
        };
        let cargo: BTreeSet<String> = listed
            .into_iter()
            .filter(|path| !is_packaging_artifact(path))
            .collect();
        if tool == cargo {
            continue;
        }
        // Naming the paths is what makes the warning actionable: the reader has
        // to decide whether a released-content rule is wrong or the tree simply
        // is not clean, and only the differing paths distinguish those.
        let only_in_tool = difference_text(&tool, &cargo);
        let only_in_cargo = difference_text(&cargo, &tool);
        writeln!(
            warnings,
            "warning: packaging rule mismatch for {}: only in tool: {only_in_tool}; \
             only in `cargo package --list`: {only_in_cargo} (Cargo.lock ignored)",
            quote_path(&package.manifest.name)
        )
        .expect("writing to String");
    }
    warnings
}

/// Renders the paths in `left` that `right` does not have.
fn difference_text(left: &BTreeSet<String>, right: &BTreeSet<String>) -> String {
    let paths: Vec<Cow<'_, str>> = left
        .difference(right)
        .map(String::as_str)
        .map(quote_path)
        .collect();
    if paths.is_empty() {
        "nothing".to_string()
    } else {
        paths.join(", ")
    }
}

/// Whether `cargo package --list` produced this entry rather than the package source.
///
/// The list mixes the package's own files with entries Cargo synthesizes while
/// building the archive: a resolved lockfile, a record of the version-control
/// state, and the pre-normalization copy of the manifest. None of those exist in
/// the work tree, so comparing them against the tool's released-content set
/// would report a mismatch on every package. The lockfile is additionally
/// excluded by policy, because it is derived when the archive is built rather
/// than being a function of the package source; only the archive-root path is
/// synthesized, so a lockfile nested deeper stays in the comparison as the
/// ordinary source file it is.
/// Ref: docs/design.md, "Released content"; Cargo's `cargo package` reference
/// for the entries it adds to an archive.
fn is_packaging_artifact(path: &str) -> bool {
    path == "Cargo.lock" || path == ".cargo_vcs_info.json" || path == "Cargo.toml.orig"
}

// Spawns `cargo package --list`; catching mutations would compile every fixture.
#[cfg_attr(test, mutants::skip)]
fn cargo_package_list(workspace_root: &Path, package: &str) -> Result<Vec<String>, AppError> {
    let stdout = run_capture(
        "cargo",
        &[
            "package",
            "--list",
            "--offline",
            "--allow-dirty",
            "-p",
            package,
        ],
        workspace_root,
    )?;
    Ok(parse_package_list(&stdout))
}

/// Parses the newline-delimited archive paths emitted by `cargo package --list`.
fn parse_package_list(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::HashSet;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::path::PathBuf;

    use static_assertions::assert_impl_all;

    use super::*;
    use crate::anchor::Anchor;
    use crate::metadata::{DepKind, ReportedDep};

    assert_impl_all!(CheckFormat: UnwindSafe, RefUnwindSafe);

    /// Stands in for whichever revision a run classified against.
    const BASE: &str = "origin/main";

    /// Package-list parsing preserves whitespace that belongs to a path.
    #[test]
    fn package_list_paths_are_preserved_verbatim() {
        assert_eq!(
            parse_package_list("ordinary.rs\n leading.rs\ntrailing.rs \n \n\n"),
            ["ordinary.rs", " leading.rs", "trailing.rs ", " ",]
        );
    }

    #[test]
    fn the_remedy_names_the_base_the_run_used() {
        // Following the remediation against a different base would report a
        // different set of packages than the one that failed.
        let package = failing("demo", Vec::new());

        let text = render_diagnostics(&[package], &BTreeMap::new(), "deadbeef", CheckFormat::Text);

        assert!(text.contains("--base <base>"));
        assert!(text.contains("base reported here: deadbeef"));
    }

    #[test]
    fn remedy_keeps_a_repository_controlled_base_out_of_commands() {
        let base = "release; echo injected";

        let text = remedy(base);
        let report_command = text
            .split('`')
            .nth(1)
            .expect("the remedy contains a report command");

        assert_eq!(
            report_command,
            "cargo release-plan report --out-dir <dir> --base <base>"
        );
        assert!(!report_command.contains(base));
    }

    #[test]
    fn default_success_message_only_when_passed_without_diagnostics() {
        assert!(default_success_message(true, "").is_some());
        assert!(default_success_message(true, "diagnostic").is_none());
        assert!(default_success_message(false, "").is_none());
        assert!(default_success_message(false, "fail").is_none());
    }

    /// Compatibility follows Cargo's leftmost-non-zero rule rather than the major component.
    #[test]
    fn compatibility_is_decided_by_the_leftmost_non_zero_component() {
        // A 1.x line breaks on the major component.
        assert_eq!(
            compatibility_key(&Version::new(1, 2, 3)),
            compatibility_key(&Version::new(1, 9, 0))
        );
        assert_ne!(
            compatibility_key(&Version::new(1, 2, 3)),
            compatibility_key(&Version::new(2, 0, 0))
        );

        // A 0.x line breaks on the minor component.
        assert_eq!(
            compatibility_key(&Version::new(0, 1, 2)),
            compatibility_key(&Version::new(0, 1, 9))
        );
        assert_ne!(
            compatibility_key(&Version::new(0, 1, 2)),
            compatibility_key(&Version::new(0, 2, 0))
        );

        // A 0.0.x line admits no compatible change at all.
        assert_ne!(
            compatibility_key(&Version::new(0, 0, 1)),
            compatibility_key(&Version::new(0, 0, 2))
        );
    }

    /// A compatible move is not a breaking release.
    #[test]
    fn a_compatible_move_releases_no_breaking_change() {
        let package = with_dependencies(
            "demo",
            Version::new(0, 1, 3),
            Version::new(0, 1, 0),
            Vec::new(),
        );

        assert!(!releases_breaking_change(&package));
    }

    /// Builds an unchanged package carrying the given intra-workspace dependencies.
    fn with_dependencies(
        name: &str,
        declared: Version,
        anchor: Version,
        dependencies: Vec<ReportedDep>,
    ) -> PackageClass {
        let mut package = PackageClass::unchanged(
            name,
            declared,
            Anchor {
                commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                version: anchor,
            },
            PathBuf::from(format!("packages/{name}/Cargo.toml")),
        );
        package.dependencies = dependencies;
        package
    }

    fn dependency(name: &str, req: &str, public: bool) -> ReportedDep {
        ReportedDep {
            name: name.to_string(),
            req: req.to_string(),
            exact_pin: req.starts_with('='),
            kind: DepKind::Normal,
            public,
        }
    }

    /// Builds an unchanged package in a version group, carrying the given dependencies.
    fn grouped(
        name: &str,
        group: &str,
        declared: Version,
        dependencies: Vec<ReportedDep>,
    ) -> PackageClass {
        let mut package = with_dependencies(name, declared.clone(), declared, dependencies);
        package.group = Some(group.to_string());
        package
    }

    /// A compatible edge is valid between members connected through exact edges elsewhere.
    #[test]
    fn a_compatible_requirement_between_group_members_is_accepted() {
        let library = grouped("lib_impl", "lib", Version::new(1, 1, 0), vec![]);
        let shell = grouped(
            "lib",
            "lib",
            Version::new(1, 1, 0),
            vec![dependency("lib_impl", "^1.1.0", true)],
        );

        let text = render_diagnostics(&[shell, library], &BTreeMap::new(), BASE, CheckFormat::Text);

        assert_eq!(text, "");
    }

    /// Packages in different groups may reference each other compatibly.
    #[test]
    fn a_compatible_requirement_across_groups_is_accepted() {
        let library = grouped("lib", "lib", Version::new(1, 1, 0), vec![]);
        let consumer = grouped(
            "app",
            "app",
            Version::new(0, 1, 0),
            vec![dependency("lib", "^1.1.0", false)],
        );

        let text = render_diagnostics(
            &[consumer, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// An ungrouped package may reference a group member compatibly.
    #[test]
    fn a_compatible_requirement_from_outside_a_group_is_accepted() {
        let library = grouped("lib", "lib", Version::new(1, 1, 0), vec![]);
        let consumer = with_dependencies(
            "app",
            Version::new(0, 1, 0),
            Version::new(0, 1, 0),
            vec![dependency("lib", "^1.1.0", false)],
        );

        let text = render_diagnostics(
            &[consumer, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// A requirement that does not name the version its target declares is rejected.
    #[test]
    fn a_requirement_that_does_not_name_the_declared_version_is_reported() {
        let library =
            with_dependencies("lib", Version::new(1, 1, 0), Version::new(1, 1, 0), vec![]);
        let dependent = with_dependencies(
            "app",
            Version::new(0, 1, 0),
            Version::new(0, 1, 0),
            vec![dependency("lib", "^1.0.0", false)],
        );

        let text = render_diagnostics(
            &[dependent, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert!(
            text.contains("does not name the version it declares"),
            "{text}"
        );
        assert!(text.contains("1.1.0"), "{text}");
    }

    /// A requirement naming the declared version passes in either accepted spelling.
    #[test]
    fn a_requirement_naming_the_declared_version_is_accepted() {
        let library =
            with_dependencies("lib", Version::new(1, 1, 0), Version::new(1, 1, 0), vec![]);
        for req in ["^1.1.0", "=1.1.0"] {
            let dependent = with_dependencies(
                "app",
                Version::new(0, 1, 0),
                Version::new(0, 1, 0),
                vec![dependency("lib", req, false)],
            );

            let text = render_diagnostics(
                &[dependent, library.clone()],
                &BTreeMap::new(),
                BASE,
                CheckFormat::Text,
            );

            assert_eq!(text, "", "requirement {req} was rejected");
        }
    }

    /// A package exposing a dependency that breaks must break too.
    ///
    /// The dependency's incompatible release changes the identity of the types this package
    /// re-exports, so staying on a compatible version would publish a contract its consumers
    /// cannot satisfy.
    #[test]
    fn a_public_dependency_releasing_a_breaking_change_forces_one_on_its_dependent() {
        // `lib` moves 1.1.0 -> 2.0.0, which is incompatible.
        let library =
            with_dependencies("lib", Version::new(2, 0, 0), Version::new(1, 1, 0), vec![]);
        let dependent = with_dependencies(
            "app",
            Version::new(0, 1, 1),
            Version::new(0, 1, 0),
            vec![dependency("lib", "^2.0.0", true)],
        );

        let text = render_diagnostics(
            &[dependent, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert!(
            text.contains("must release a breaking change of its own"),
            "{text}"
        );
    }

    /// The same dependency does not force anything when it is not publicly exposed.
    #[test]
    fn a_private_dependency_releasing_a_breaking_change_forces_nothing() {
        let library =
            with_dependencies("lib", Version::new(2, 0, 0), Version::new(1, 1, 0), vec![]);
        let dependent = with_dependencies(
            "app",
            Version::new(0, 1, 1),
            Version::new(0, 1, 0),
            vec![dependency("lib", "^2.0.0", false)],
        );

        let text = render_diagnostics(
            &[dependent, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// Each manifest-level diagnostic carries a workflow annotation naming its manifest.
    ///
    /// The annotation is what attaches a failure to the offending file in the pull-request
    /// view, so a wrong title or an unescaped body would leave CI reporting into the void
    /// while the text form still looked correct.
    #[test]
    fn github_format_annotates_each_manifest_diagnostic() {
        // A drifted requirement, from a package in no group.
        let stale = render_diagnostics(
            &[
                with_dependencies(
                    "app",
                    Version::new(0, 1, 0),
                    Version::new(0, 1, 0),
                    vec![dependency("lib", "^1.0.0", false)],
                ),
                with_dependencies("lib", Version::new(1, 1, 0), Version::new(1, 1, 0), vec![]),
            ],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Github,
        );
        assert!(
            stale.contains(
                "::error file=packages/app/Cargo.toml,title=stale-workspace-requirement::"
            ),
            "{stale}"
        );

        // A public dependency that breaks while its dependent stays compatible.
        let unpropagated = render_diagnostics(
            &[
                with_dependencies(
                    "app",
                    Version::new(0, 1, 1),
                    Version::new(0, 1, 0),
                    vec![dependency("lib", "=2.0.0", true)],
                ),
                with_dependencies("lib", Version::new(2, 0, 0), Version::new(1, 1, 0), vec![]),
            ],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Github,
        );
        assert!(
            unpropagated.contains(
                "::error file=packages/app/Cargo.toml,title=unpropagated-breaking-change::"
            ),
            "{unpropagated}"
        );
    }

    /// A drifted exact pin is corrected to an exact pin, not to a bare version.
    ///
    /// The suggested spelling has to keep the lockstep the author asked for, otherwise
    /// following the diagnostic would silently widen the requirement.
    #[test]
    fn a_drifted_exact_pin_is_corrected_to_an_exact_pin() {
        let text = render_diagnostics(
            &[
                with_dependencies(
                    "app",
                    Version::new(0, 1, 0),
                    Version::new(0, 1, 0),
                    vec![dependency("lib", "=1.0.0", false)],
                ),
                with_dependencies("lib", Version::new(1, 1, 0), Version::new(1, 1, 0), vec![]),
            ],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert!(text.contains("Change the requirement to =1.1.0"), "{text}");
    }

    /// A dependency on a package outside the assessed set is not judged.
    ///
    /// Only publishable, Git-tracked packages carry a declared version here, so an edge whose
    /// target is absent has nothing to compare against.
    #[test]
    fn a_dependency_on_an_unassessed_package_is_skipped() {
        let text = render_diagnostics(
            &[with_dependencies(
                "app",
                Version::new(0, 1, 0),
                Version::new(0, 1, 0),
                vec![dependency("absent", "^1.0.0", true)],
            )],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// A never-published dependent owes no breaking release.
    ///
    /// It has no consumer contract to break, and no anchor to move away from, so demanding one
    /// would be a diagnostic it could never satisfy: no version it declares would clear the
    /// rule. It takes the first-publication path instead.
    #[test]
    fn a_never_published_dependent_exposing_a_breaking_dependency_is_accepted() {
        let library =
            with_dependencies("lib", Version::new(2, 0, 0), Version::new(1, 1, 0), vec![]);
        let mut newcomer = PackageClass::new_package(
            "app",
            Version::new(0, 1, 0),
            PathBuf::from("packages/app/Cargo.toml"),
        );
        newcomer.dependencies = vec![dependency("lib", "=2.0.0", true)];

        let text = render_diagnostics(
            &[newcomer, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// A dependent that already releases a breaking change of its own satisfies the rule.
    #[test]
    fn a_dependent_already_releasing_a_breaking_change_is_accepted() {
        let library =
            with_dependencies("lib", Version::new(2, 0, 0), Version::new(1, 1, 0), vec![]);
        // 0.1.0 -> 0.2.0 is incompatible on a 0.x line.
        let dependent = with_dependencies(
            "app",
            Version::new(0, 2, 0),
            Version::new(0, 1, 0),
            vec![dependency("lib", "^2.0.0", true)],
        );

        let text = render_diagnostics(
            &[dependent, library],
            &BTreeMap::new(),
            BASE,
            CheckFormat::Text,
        );

        assert_eq!(text, "");
    }

    /// Builds a package that renders a diagnostic, with the rest left inert.
    ///
    /// Only status, name, anchor, group, and changed items reach the rendered
    /// text, so every other field carries a value the assertions never observe.
    fn failing(name: &str, changed: Vec<ChangedItem>) -> PackageClass {
        PackageClass::needs_increment(
            name,
            Version::new(0, 1, 0),
            Anchor {
                commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                version: Version::new(0, 1, 0),
            },
            changed,
            PathBuf::from("packages/demo/Cargo.toml"),
        )
    }

    #[test]
    fn released_packages_produce_no_diagnostics() {
        let package = PackageClass::unchanged(
            "demo",
            Version::new(0, 1, 0),
            Anchor {
                commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                version: Version::new(0, 1, 0),
            },
            PathBuf::from("packages/demo/Cargo.toml"),
        );

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Text);

        assert_eq!(text, "");
    }

    #[test]
    fn a_changed_path_is_named_ahead_of_an_inherited_field() {
        let package = failing(
            "demo",
            vec![
                ChangedItem::Package {
                    path: "src/lib.rs".to_string(),
                    change: "modified".to_string(),
                },
                ChangedItem::Inherited {
                    field: "package.rust-version".to_string(),
                },
            ],
        );

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Text);

        assert!(
            text.contains("src/lib.rs (and related paths) changed"),
            "{text}"
        );
    }

    #[test]
    fn an_inherited_field_is_named_when_it_is_the_only_change() {
        let package = failing(
            "demo",
            vec![ChangedItem::Inherited {
                field: "package.rust-version".to_string(),
            }],
        );

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Text);

        assert!(
            text.contains("package.rust-version (and related paths) changed"),
            "{text}"
        );
    }

    /// A package without changed items still reports.
    ///
    /// A package can reach `needs-increment` through an inherited value alone, which leaves no
    /// changed path to name.
    #[test]
    fn a_package_without_changed_items_still_reports() {
        let package = failing("demo", Vec::new());

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Text);

        assert!(text.contains("released content changed"), "{text}");
    }

    #[test]
    fn a_grouped_package_names_the_other_members() {
        let mut package = failing("demo", Vec::new());
        package.group = Some("g".to_string());
        let groups = BTreeMap::from([(
            "g".to_string(),
            GroupVerdict::new(
                &["demo".to_string(), "sibling".to_string()],
                &BTreeMap::from([
                    ("demo".to_string(), Version::new(0, 1, 0)),
                    ("sibling".to_string(), Version::new(0, 1, 0)),
                ]),
                &HashSet::new(),
            ),
        )]);

        let text = render_diagnostics(&[package], &groups, BASE, CheckFormat::Text);

        assert!(
            text.contains("Group g also includes demo, sibling."),
            "{text}"
        );
    }

    #[test]
    fn an_inconsistent_group_names_the_version_each_member_declares() {
        let member = PackageClass::pending_release(
            "demo",
            Version::new(0, 2, 0),
            Anchor {
                commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                version: Version::new(0, 1, 0),
            },
            PathBuf::from("packages/demo/Cargo.toml"),
        );
        let groups = BTreeMap::from([(
            "g".to_string(),
            GroupVerdict::new(
                // One member is not among the classified packages, so it has no
                // declared version to report and must still appear in the list.
                // The versions here are arbitrary; only the presence of a member
                // without a classification matters.
                &["demo".to_string(), "absent".to_string()],
                &BTreeMap::from([
                    ("demo".to_string(), Version::new(0, 2, 0)),
                    ("absent".to_string(), Version::new(0, 1, 0)),
                ]),
                &HashSet::new(),
            ),
        )]);

        let text = render_diagnostics(&[member], &groups, BASE, CheckFormat::Text);

        assert!(text.contains("demo@0.2.0"), "{text}");
        assert!(text.contains("absent"), "{text}");
        assert!(!text.contains("absent@"), "{text}");
    }

    #[test]
    fn github_format_precedes_each_diagnostic_with_an_annotation() {
        let package = failing("demo", Vec::new());

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Github);

        let mut lines = text.lines();
        assert!(
            lines
                .next()
                .expect("a failing package renders at least one line")
                .starts_with("::error file=packages/demo/Cargo.toml,title=needs-increment::"),
            "{text}"
        );
        assert!(
            lines
                .next()
                .expect("the annotation is followed by the plain diagnostic")
                .starts_with("demo: needs-increment"),
            "{text}"
        );
    }

    #[test]
    fn packaging_differences_name_the_paths_on_each_side() {
        let tool = BTreeSet::from(["src/lib.rs".to_string(), "Cargo.toml".to_string()]);
        let cargo = BTreeSet::from(["Cargo.toml".to_string(), "src/extra.rs".to_string()]);

        assert_eq!(difference_text(&tool, &cargo), "src/lib.rs");
        assert_eq!(difference_text(&cargo, &tool), "src/extra.rs");
        assert_eq!(difference_text(&tool, &tool), "nothing");
    }

    /// A repository controlled name cannot break out of a diagnostic.
    ///
    /// Diagnostics are read from a CI log, where a name carrying a newline would let the tail of a
    /// repository-controlled path pose as a fresh workflow command and a terminal escape could
    /// rewrite what a reader sees.
    #[test]
    fn a_repository_controlled_name_cannot_break_out_of_a_diagnostic() {
        let package = failing(
            "demo",
            vec![ChangedItem::Package {
                path: "src/\n::error::spoofed".to_string(),
                change: "modified".to_string(),
            }],
        );

        let text = render_diagnostics(&[package], &BTreeMap::new(), BASE, CheckFormat::Github);

        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(text.contains(r#""src/\n::error::spoofed""#), "{text}");

        let odd = BTreeSet::from(["src/\u{1b}[2Kgone.rs".to_string()]);
        assert_eq!(
            difference_text(&odd, &BTreeSet::new()),
            r#""src/\033[2Kgone.rs""#
        );
    }

    #[test]
    fn packaging_artifacts_are_ignored_in_verify() {
        assert!(is_packaging_artifact("Cargo.lock"));
        // Only the package-root lockfile is synthesized at pack time.
        assert!(!is_packaging_artifact("fixtures/Cargo.lock"));
        assert!(is_packaging_artifact(".cargo_vcs_info.json"));
        assert!(is_packaging_artifact("Cargo.toml.orig"));
        assert!(!is_packaging_artifact("src/lib.rs"));
    }

    #[test]
    fn workflow_command_data_is_escaped() {
        assert_eq!(escape_data("100% done"), "100%25 done");
        assert_eq!(escape_data("a\r\nb"), "a%0D%0Ab");
        // A colon or comma is only a delimiter in a property, not in the body.
        assert_eq!(escape_data("a:b,c"), "a:b,c");
    }

    #[test]
    fn workflow_command_properties_escape_their_delimiters() {
        assert_eq!(
            escape_property("odd,name/100%/a:b/Cargo.toml"),
            "odd%2Cname/100%25/a%3Ab/Cargo.toml"
        );
    }
}
