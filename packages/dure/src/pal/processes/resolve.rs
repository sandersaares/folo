//! Resolving the app command to an executable image.
//!
//! `CreateProcessW` performs no search of its own when the caller supplies an
//! explicit application name, and letting it fall back to its command-line
//! parsing would hand the decision to ambient process state. Resolution is
//! therefore done here, once, so the client can also explain it before the
//! supervisor exists (design.md, "Commands"; design.md, "Diagnostics").

use std::path::{Component, Path, PathBuf};

use super::windows::search_path;

/// The extension a bare command name is completed with.
///
/// `dure` supervises an app it owns and waits on, so the command has to be a
/// directly executable image; a script wrapper is launched through its
/// interpreter instead (design.md, "Commands").
const EXECUTABLE_EXTENSION: &str = ".exe";

/// How an app command was turned into a path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HowResolved {
    /// The command already named an absolute path.
    Absolute,
    /// A path with a separator, taken relative to the launch directory.
    RelativeToLaunchDirectory,
    /// A bare name found on the executable search path.
    SearchPath,
    /// A bare name the search path does not have.
    NotFound,
}

/// Where an app command points, and how that was decided.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedCommand {
    /// The path handed to `CreateProcessW`.
    pub path: PathBuf,
    /// Which rule produced it.
    pub how: HowResolved,
}

/// Resolves `command` to the executable image `dure run` will launch.
pub(crate) fn resolve_executable(command: &str, launch_directory: &Path) -> ResolvedCommand {
    let path = Path::new(command);
    if path.is_absolute() {
        return ResolvedCommand {
            path: path.to_path_buf(),
            how: HowResolved::Absolute,
        };
    }
    if has_separator(command) || is_drive_relative(path) {
        // A drive-relative path such as `C:tools\app.exe` has a prefix but no
        // root, so joining it would preserve the ambient per-drive current
        // directory Windows keeps. The session's launch directory is the
        // recorded one, so the drive prefix is dropped and the rest is anchored
        // there. Ref: docs/design.md, "Commands".
        let relative = strip_drive_prefix(path);
        return ResolvedCommand {
            path: launch_directory.join(relative),
            how: HowResolved::RelativeToLaunchDirectory,
        };
    }
    match search_path(command, EXECUTABLE_EXTENSION) {
        Some(found) => ResolvedCommand {
            path: found,
            how: HowResolved::SearchPath,
        },
        None => ResolvedCommand {
            path: path.to_path_buf(),
            how: HowResolved::NotFound,
        },
    }
}

fn has_separator(command: &str) -> bool {
    command.contains('/') || command.contains('\\')
}

fn is_drive_relative(path: &Path) -> bool {
    matches!(path.components().next(), Some(Component::Prefix(_)))
}

/// Drops a drive prefix so what remains can be anchored somewhere else.
fn strip_drive_prefix(path: &Path) -> PathBuf {
    path.components()
        .skip_while(|component| matches!(component, Component::Prefix(_) | Component::RootDir))
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    const LAUNCH: &str = r"C:\work";

    fn resolve(command: &str) -> ResolvedCommand {
        resolve_executable(command, Path::new(LAUNCH))
    }

    #[test]
    fn an_absolute_path_is_taken_as_written() {
        let resolved = resolve(r"D:\tools\app.exe");
        assert_eq!(resolved.path, PathBuf::from(r"D:\tools\app.exe"));
        assert_eq!(resolved.how, HowResolved::Absolute);
    }

    #[test]
    fn a_path_with_a_separator_is_anchored_at_the_launch_directory() {
        for command in [r"bin\app.exe", "bin/app.exe", r".\app.exe"] {
            let resolved = resolve(command);
            assert_eq!(resolved.how, HowResolved::RelativeToLaunchDirectory);
            assert!(
                resolved.path.starts_with(LAUNCH),
                "{command} resolved to {}",
                resolved.path.display()
            );
        }
    }

    #[test]
    fn a_drive_relative_path_is_anchored_at_the_launch_directory() {
        // `C:tools\app.exe` would otherwise follow the per-drive current
        // directory Windows keeps, which is not the session's launch directory.
        let resolved = resolve(r"C:tools\app.exe");
        assert_eq!(resolved.how, HowResolved::RelativeToLaunchDirectory);
        assert_eq!(resolved.path, PathBuf::from(r"C:\work\tools\app.exe"));
    }

    #[test]
    fn a_drive_prefix_alone_is_enough_to_anchor_a_path() {
        // No separator, so the drive prefix is the only thing distinguishing
        // this from a bare name that would be looked up on the search path.
        let resolved = resolve("C:app.exe");
        assert_eq!(resolved.how, HowResolved::RelativeToLaunchDirectory);
        assert_eq!(resolved.path, PathBuf::from(r"C:\work\app.exe"));
    }

    #[test]
    // Talks to the real operating system: reads the process search path.
    #[cfg_attr(miri, ignore)]
    fn a_bare_name_is_looked_up_on_the_search_path() {
        // Present on every Windows installation this can run on.
        let resolved = resolve("cmd");
        assert_eq!(resolved.how, HowResolved::SearchPath);
        assert!(resolved.path.is_absolute());
        // A bare name must not be satisfied from the launch directory.
        assert!(!resolved.path.starts_with(LAUNCH));
    }

    #[test]
    // Talks to the real operating system: reads the process search path.
    #[cfg_attr(miri, ignore)]
    fn a_bare_name_that_is_nowhere_keeps_its_own_spelling() {
        let resolved = resolve("no-such-executable-anywhere");
        assert_eq!(resolved.how, HowResolved::NotFound);
        assert_eq!(resolved.path, PathBuf::from("no-such-executable-anywhere"));
    }
}
