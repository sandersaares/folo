//! Process, job, and supervisor-spawn PAL.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::AppCommand;
use crate::durability::LauncherTie;
use crate::pal::error::PalError;
use crate::pal::ids::{AppId, JobId, PtyId};
use crate::pal::processes::ResolvedCommand;
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
/// Ref: docs/implementation.md, "PAL slicing" and "Detached supervisor".
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
    /// Ref: docs/job-breakaway.md.
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
    ///
    /// Consumes the app: the handle it names is released here, so the id must
    /// not be used again.
    fn wait_app(&self, app: AppId) -> Result<i32, PalError>;

    /// Identity of the current process, used when the supervisor publishes its record.
    fn current_identity(&self) -> Result<ProcessIdentity, PalError>;

    /// Generate a random nonce for pipe names.
    fn random_nonce(&self) -> String;

    /// Where a command points and how that was decided.
    ///
    /// Answered before the app is spawned so a caller can explain the choice,
    /// and used by the spawn itself, so the two can never disagree.
    fn resolve_executable(&self, command: &str, launch_directory: &Path) -> ResolvedCommand;
}
