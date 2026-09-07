// Subprocess helper for `git` and `cargo`.
//
// Classification is specified to shell out rather than link libgit2/gix or a
// Cargo library, so this is the only subprocess boundary.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, ExitStatus, Output};

use ohno::AppError;

use crate::{CommandFailedError, CommandSpawnError};

/// Runs `program` with `args` in `cwd` and returns UTF-8 stdout on success.
///
/// # Errors
///
/// Returns [`CommandSpawnError`] if the process cannot be created, or
/// [`CommandFailedError`] if it exits unsuccessfully.
pub(crate) fn run_capture(program: &str, args: &[&str], cwd: &Path) -> Result<String, AppError> {
    run_capture_os(program, args, cwd)
}

/// Runs `program` with OS-str arguments (needed for git pathspecs on Windows).
///
/// Stdout is decoded lossily, so this is for output that is not a file name:
/// path listings go through [`run_capture_os_bytes`] and are decoded strictly,
/// because replacing a byte in a name would silently name a different file.
pub(crate) fn run_capture_os(
    program: &str,
    args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    cwd: &Path,
) -> Result<String, AppError> {
    let output = spawn(program, args, cwd)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(CommandFailedError::new(
            program,
            failure_status(output.status),
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        )
        .into())
    }
}

/// Like [`run_capture`], mapping a non-zero exit to `Ok(None)`.
///
/// Spawn failures still error.
pub(crate) fn run_capture_ok(
    program: &str,
    args: &[&str],
    cwd: &Path,
) -> Result<Option<String>, AppError> {
    match run_capture_ok_bytes(program, args, cwd)? {
        Some(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        None => Ok(None),
    }
}

/// Runs `program` and returns raw stdout on success.
pub(crate) fn run_capture_bytes(
    program: &str,
    args: &[&str],
    cwd: &Path,
) -> Result<Vec<u8>, AppError> {
    run_capture_os_bytes(program, args.iter().map(OsStr::new), cwd)
}

/// Runs `program` with OS-str arguments and returns raw stdout on success.
pub(crate) fn run_capture_os_bytes(
    program: &str,
    args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    cwd: &Path,
) -> Result<Vec<u8>, AppError> {
    let output = spawn(program, args, cwd)?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(CommandFailedError::new(
            program,
            failure_status(output.status),
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        )
        .into())
    }
}

/// Like [`run_capture_ok`], keeping stdout as raw bytes.
///
/// Binary files can then be compared without UTF-8 replacement.
pub(crate) fn run_capture_ok_bytes(
    program: &str,
    args: &[&str],
    cwd: &Path,
) -> Result<Option<Vec<u8>>, AppError> {
    match run_capture_bytes(program, args, cwd) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) => {
            if error.find_source::<CommandFailedError>().is_some() {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

/// Renders either the exit code or the signal-only fallback.
fn failure_status(status: ExitStatus) -> String {
    status
        .code()
        .map_or_else(|| "signal".to_string(), |code| code.to_string())
}

/// Directory passed to `Command::current_dir`.
///
/// `Path::parent()` of a relative file such as `Cargo.toml` is the empty path.
/// `Command::current_dir` rejects that empty path (Windows `ERROR_INVALID_NAME`,
/// Unix `chdir("")`), so use the process current directory instead.
fn subprocess_cwd(cwd: &Path) -> &Path {
    if cwd.as_os_str().is_empty() {
        Path::new(".")
    } else {
        cwd
    }
}

// Mutations of the spawn arguments cannot be caught without asserting on a real
// child process, which is impractical in unit tests.
#[cfg_attr(test, mutants::skip)]
fn spawn(
    program: &str,
    args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    cwd: &Path,
) -> Result<Output, AppError> {
    // Every child here is captured through pipes and its output is parsed or
    // surfaced verbatim in diagnostics, so it must be free of ANSI escapes. The
    // override belongs on the shared boundary rather than at each call site
    // because Cargo's automatic detection depends on the ambient environment,
    // which would otherwise make captured output differ between a terminal, a
    // CI runner and a test harness.
    //
    // The locale is pinned for the same reason: Git translates its diagnostics,
    // and `git.rs` recognises a path that is absent from a revision by the
    // wording Git uses. Under a translated locale that wording never matches and
    // an ordinary package creation or deletion would surface as an operational
    // error. GNU gettext ignores `LANGUAGE` once the locale is `C`, so this one
    // variable settles it.
    Command::new(program)
        .args(args)
        .current_dir(subprocess_cwd(cwd))
        .env("CARGO_TERM_COLOR", "never")
        .env("LC_ALL", "C")
        .output()
        .map_err(|error| CommandSpawnError::caused_by(program, error).into())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt as _;

    use super::*;
    use crate::CommandSpawnError;

    #[test]
    fn empty_cwd_uses_process_current_directory() {
        assert_eq!(subprocess_cwd(Path::new("")), Path::new("."));
        assert_eq!(subprocess_cwd(Path::new(".")), Path::new("."));
        assert_eq!(subprocess_cwd(Path::new("packages")), Path::new("packages"));
    }

    #[cfg_attr(miri, ignore)] // Process spawn uses host APIs Miri cannot emulate.
    #[test]
    fn run_capture_ok_returns_stdout_on_success() {
        let stdout = run_capture_ok(
            "git",
            &["rev-parse", "--is-inside-work-tree"],
            Path::new("."),
        )
        .unwrap();
        assert_eq!(stdout.as_deref().map(str::trim), Some("true"));
    }

    #[cfg_attr(miri, ignore)] // Process spawn uses host APIs Miri cannot emulate.
    #[test]
    fn run_capture_ok_none_on_nonzero_exit() {
        let stdout = run_capture_ok(
            "git",
            &["rev-parse", "--verify", "cargo-release-plan-no-such-rev"],
            Path::new("."),
        )
        .unwrap();
        assert!(stdout.is_none());
    }

    #[cfg_attr(miri, ignore)] // Process spawn uses host APIs Miri cannot emulate.
    #[test]
    fn run_capture_ok_propagates_a_spawn_failure() {
        // A non-zero exit means "no answer", but never starting the program at
        // all is a real error the caller must see.
        let error =
            run_capture_ok("cargo-release-plan-no-such-program", &[], Path::new(".")).unwrap_err();
        assert!(error.find_source::<CommandSpawnError>().is_some());
    }

    #[cfg_attr(miri, ignore)] // Process spawn uses host APIs Miri cannot emulate.
    #[test]
    fn spawn_failure_maps_to_command_spawn_error() {
        // A program name that cannot exist on PATH cannot be spawned.
        let error = spawn(
            "cargo-release-plan-no-such-program",
            None::<&OsStr>,
            Path::new("."),
        )
        .unwrap_err();
        assert!(error.find_source::<CommandSpawnError>().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn a_signal_only_exit_has_a_stable_status() {
        // POSIX wait status for a process terminated by SIGTERM.
        let status = ExitStatus::from_raw(15);
        assert_eq!(failure_status(status), "signal");
    }
}
