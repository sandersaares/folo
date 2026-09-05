//! Process, job, and supervisor-spawn PAL.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::app_command::AppCommand;
use crate::durability::LauncherTie;
use crate::pal::error::PalError;
use crate::pal::ids::{AppId, JobId, PtyId};
use crate::session_record::ProcessIdentity;

/// Outcome of probing a recorded supervisor process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum ProcessLiveness {
    /// The same process is still running.
    Live,
    /// Missing, exited, or pid reused by a different process.
    Dead,
    /// The process could not be inspected; the record must be kept.
    InspectFailed,
}

/// Request to spawn a console-detached supervisor with job breakaway.
#[derive(Clone, Debug)]
pub(crate) struct SupervisorSpawn {
    /// Path of this `dure` binary.
    pub exe: PathBuf,
    /// Arguments after the program name, including the hidden subcommand.
    pub args: Vec<String>,
}

/// Request to spawn the app attached to a pseudoconsole and lifetime job.
#[derive(Clone, Debug)]
pub(crate) struct AppSpawn {
    /// Command argv.
    pub command: AppCommand,
    /// Working directory and relative-path resolution root.
    pub launch_directory: PathBuf,
    /// Pseudoconsole the app should attach to.
    pub pty: PtyId,
    /// Kill-on-close job the app is born into.
    pub job: JobId,
}

/// Spawn a detached supervisor, identify processes, own the app-lifetime job.
///
/// Ref: docs/implementation.md, PAL slicing and "Detached supervisor".
#[cfg_attr(test, mockall::automock)]
pub(crate) trait Processes: Send + Sync + fmt::Debug + 'static {
    /// Path of the current executable, used to re-spawn as supervisor.
    fn current_exe(&self) -> Result<PathBuf, PalError>;

    /// Spawn a supervisor that is not in the caller's kill-on-close job.
    fn spawn_supervisor(&self, request: &SupervisorSpawn) -> Result<ProcessIdentity, PalError>;

    /// What the job this process is directly in says about its lifetime.
    ///
    /// Breakaway leaves only the immediate job, so the supervisor asks about the
    /// job it actually landed in. Windows reports job membership only to the
    /// process itself, so no other process can answer this, and it exposes no
    /// ancestor jobs, so an outer job is never ruled out.
    ///
    /// Ref: docs/implementation.md, "Job breakaway".
    fn launcher_tie(&self) -> LauncherTie;

    /// Open the pid, verify creation time, and report whether it is running.
    fn probe(&self, identity: &ProcessIdentity) -> ProcessLiveness;

    /// Terminate a verified process handle. Pid reuse cannot kill a replacement.
    fn terminate(&self, identity: &ProcessIdentity) -> Result<(), PalError>;

    /// Create a non-inheritable kill-on-close job that still allows breakaway.
    fn create_lifetime_job(&self) -> Result<JobId, PalError>;

    /// Close the job. Kill-on-close ends assigned processes.
    fn close_job(&self, job: JobId);

    /// Spawn the app attached to the pseudoconsole and assigned to the job.
    fn spawn_app(&self, request: &AppSpawn) -> Result<AppId, PalError>;

    /// Block until the app exits and return its status.
    fn wait_app(&self, app: AppId) -> Result<i32, PalError>;

    /// Identity of the current process, used when the supervisor publishes its record.
    fn current_identity(&self) -> Result<ProcessIdentity, PalError>;

    /// Generate a random nonce for pipe names.
    fn random_nonce(&self) -> String;
}

/// Resolves `exe` relative to `launch_directory` when it contains a path separator.
///
/// A bare name is left alone here and resolved through the platform's
/// executable search order when the app is spawned.
#[must_use]
pub(crate) fn resolve_command_path(command: &str, launch_directory: &Path) -> PathBuf {
    let path = Path::new(command);
    if path.is_absolute() {
        path.to_path_buf()
    } else if command.contains('/') || command.contains('\\') {
        launch_directory.join(path)
    } else {
        path.to_path_buf()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn resolve_command_path_joins_relative_with_separator() {
        let dir = Path::new("/work");
        assert_eq!(
            resolve_command_path("bin/app.exe", dir),
            PathBuf::from("/work/bin/app.exe")
        );
        assert_eq!(
            resolve_command_path("app.exe", dir),
            PathBuf::from("app.exe")
        );
        assert_eq!(
            resolve_command_path("/abs/app.exe", dir),
            PathBuf::from("/abs/app.exe")
        );
    }
}
