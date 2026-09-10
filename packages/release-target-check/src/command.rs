use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, Stdio};

use ohno::AppError;

/// Executes a read-only Git or Cargo query without shell interpretation.
pub(crate) fn capture(
    program: &str,
    arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
    cwd: &Path,
) -> Result<Vec<u8>, AppError> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(cwd)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("CARGO_TERM_COLOR", "never")
        .stdin(Stdio::null())
        .output()
        .map_err(|error| ProcessIoError::caused_by(program, error))?;
    if !output.status.success() {
        return Err(ProcessFailedError::new(
            program,
            output.status.to_string(),
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )
        .into());
    }
    Ok(output.stdout)
}

pub(crate) fn git(
    arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
    cwd: &Path,
) -> Result<String, AppError> {
    String::from_utf8(capture("git", arguments, cwd)?)
        .map_err(|error| ProcessIoError::caused_by("git", error).into())
}

/// Provides operation context when a subprocess cannot be started or read.
#[ohno::error]
#[display("cannot execute or decode {program}")]
struct ProcessIoError {
    program: String,
}

/// Retains unsuccessful subprocess diagnostics without treating them as evidence.
#[ohno::error]
#[display("{program} failed ({status}): {diagnostics}")]
struct ProcessFailedError {
    program: String,
    status: String,
    diagnostics: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fs;
    use std::io::Error;
    use std::string::FromUtf8Error;

    use tempfile::TempDir;
    use testing::with_watchdog;

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn captures_successful_output() {
        with_watchdog(|| {
            let directory = TempDir::new().unwrap();
            let output = capture("git", ["--version"], directory.path()).unwrap();
            assert!(!output.is_empty());
            assert!(
                String::from_utf8(output)
                    .unwrap()
                    .starts_with("git version ")
            );
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn preserves_unsuccessful_exit_as_a_process_failure() {
        with_watchdog(|| {
            let directory = TempDir::new().unwrap();
            let error = capture("git", ["--not-a-git-option"], directory.path()).unwrap_err();
            assert!(error.find_source::<ProcessFailedError>().is_some());
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Starts a subprocess with an absent working directory")]
    fn preserves_process_start_failure() {
        with_watchdog(|| {
            let directory = TempDir::new().unwrap();
            let error =
                capture("git", ["--version"], &directory.path().join("absent")).unwrap_err();
            assert!(error.find_source::<ProcessIoError>().is_some());
            assert!(error.find_source::<Error>().is_some());
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn rejects_non_utf8_text_output() {
        with_watchdog(|| {
            let directory = TempDir::new().unwrap();
            // Git config values are byte strings; this is not valid UTF-8 text.
            fs::write(
                directory.path().join("fixture.config"),
                b"[probe]\nvalue = \xff\n",
            )
            .unwrap();
            let error = git(
                ["config", "--file", "fixture.config", "--get", "probe.value"],
                directory.path(),
            )
            .unwrap_err();
            assert!(error.find_source::<ProcessIoError>().is_some());
            assert!(error.find_source::<FromUtf8Error>().is_some());
        });
    }
}
