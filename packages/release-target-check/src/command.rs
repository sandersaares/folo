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
