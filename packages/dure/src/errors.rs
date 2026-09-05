//! Private failure conditions of the tool.
//!
//! Each condition reaches the application boundary through `ohno::AppError`.

use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::PathBuf;

use crate::SessionId;
use crate::protocol::StartupStep;

// `ohno::error` leaves hold no shared mutable state. Empty impls match the
// workspace unwind-safety contract (docs/unwind-safety.md).
macro_rules! unwind_safe {
    ($($t:ty),+ $(,)?) => {
        $(
            impl UnwindSafe for $t {}
            impl RefUnwindSafe for $t {}
        )+
    };
}

/// Attaching requires a console.
#[ohno::error]
#[display("Attaching requires a console")]
pub(crate) struct NoConsoleError;

/// `dure run` was given no command to execute.
#[ohno::error]
#[display("dure run requires a command to execute")]
pub(crate) struct EmptyCommandError;

/// There is no live session to resume.
#[ohno::error]
#[display("No live sessions")]
pub(crate) struct NoLiveSessionsError;

/// The requested session id is not a live session.
#[ohno::error]
#[display("Session {id} is not a live session")]
pub(crate) struct SessionNotFoundError {
    id: u32,
}

impl SessionNotFoundError {
    pub(crate) fn for_id(id: SessionId) -> Self {
        Self::new(id.get())
    }
}

/// Resume did not connect within the bounded wait.
#[ohno::error]
#[display("Timed out connecting to session {id}")]
pub(crate) struct ResumeTimeoutError {
    id: u32,
}

impl ResumeTimeoutError {
    pub(crate) fn for_id(id: SessionId) -> Self {
        Self::new(id.get())
    }
}

/// The client could not complete the attach handshake.
#[ohno::error]
#[display("Failed to attach to session {id}; it may still be running")]
pub(crate) struct AttachFailedError {
    id: u32,
}

impl AttachFailedError {
    pub(crate) fn for_id(id: SessionId) -> Self {
        Self::new(id.get())
    }
}

/// Kill could not terminate the recorded supervisor.
#[ohno::error]
#[display("Failed to terminate session {id}")]
pub(crate) struct KillFailedError {
    id: u32,
}

impl KillFailedError {
    pub(crate) fn for_id(id: SessionId) -> Self {
        Self::new(id.get())
    }
}

/// The launcher's job denied breakaway, so the session could not outlive it.
#[ohno::error]
#[display(
    "Cannot start a durable session: this process belongs to a Windows job object that \
     forbids breakaway, so the supervisor would be killed together with the launcher. \
     Launch dure.exe directly instead of through a wrapper such as `cargo run`."
)]
pub(crate) struct BreakawayDeniedError;

/// Supervisor initialization failed.
#[ohno::error]
#[display("Failed to start the session")]
pub(crate) struct StartupFailedError;

/// Supervisor initialization failed, and said which step it stopped at.
#[ohno::error]
#[display("Failed to start the session while {step}")]
pub(crate) struct StartupStepFailedError {
    step: &'static str,
}

impl StartupStepFailedError {
    pub(crate) fn at(step: StartupStep) -> Self {
        Self::new(step.describe())
    }
}

/// The process working directory could not be determined.
#[ohno::error]
#[display("Failed to determine the current directory")]
pub(crate) struct CurrentDirectoryError;

/// Canonicalizing a path failed.
#[ohno::error]
#[display("Could not canonicalize '{}'", path.display())]
pub(crate) struct CanonicalizeError {
    path: PathBuf,
}

/// Session store I/O failed.
#[ohno::error]
#[display("Session store error")]
pub(crate) struct StoreError;

/// Resume without an id needs a terminal stdin to prompt for one.
#[ohno::error]
#[display("Cannot prompt for a session id without a terminal stdin; run `dure resume <id>`")]
pub(crate) struct PromptFailedError;

/// This client was displaced by a newer attach.
#[ohno::error]
#[display("Session taken by another client")]
pub(crate) struct DisplacedError;

/// Inspecting the recorded supervisor process failed.
#[ohno::error]
#[display("Failed to inspect supervisor process {pid}")]
pub(crate) struct InspectProcessError {
    pid: u32,
}

/// A PAL operation failed for a reason that is not one of the semantic cases.
#[ohno::error]
#[display("Platform operation failed")]
pub(crate) struct PalFailedError;

/// The attached console relay failed for a reason other than a normal detach.
#[ohno::error]
#[display("Console relay failed")]
pub(crate) struct RelayFailedError;

/// The supervisor went away without reporting the app's exit status.
///
/// A supervisor ends a relay by saying why — the app exited, or another client
/// took the session. A connection that simply closes means the supervisor is
/// gone, so the app's outcome is unknown and cannot be reported as success.
#[ohno::error]
#[display("Lost the session before the app reported an exit status")]
pub(crate) struct SupervisorLostError;

/// The console could not be handed back the way it was found.
#[ohno::error]
#[display("Failed to restore the console; run `cmd /c cls` or open a new terminal")]
pub(crate) struct ConsoleRestoreError;

/// The session was started by a different build of `dure`.
#[ohno::error]
#[display(
    "Session {id} was started by a different version of dure and cannot be resumed by this one; \
     use `dure kill {id}` to end it"
)]
pub(crate) struct ProtocolMismatchError {
    id: u32,
}

impl ProtocolMismatchError {
    pub(crate) fn for_id(id: SessionId) -> Self {
        Self::new(id.get())
    }
}

/// Output the command was asked to produce could not be written.
#[ohno::error]
#[display("Failed to write command output")]
pub(crate) struct OutputFailedError;

/// The user entered a session id that is not a positive integer.
#[ohno::error]
#[display("Invalid session id")]
pub(crate) struct InvalidSessionIdError;

unwind_safe!(
    NoConsoleError,
    EmptyCommandError,
    NoLiveSessionsError,
    SessionNotFoundError,
    ResumeTimeoutError,
    AttachFailedError,
    KillFailedError,
    BreakawayDeniedError,
    StartupFailedError,
    CurrentDirectoryError,
    CanonicalizeError,
    StoreError,
    PromptFailedError,
    DisplacedError,
    InspectProcessError,
    PalFailedError,
    RelayFailedError,
    SupervisorLostError,
    ConsoleRestoreError,
    OutputFailedError,
    ProtocolMismatchError,
    StartupStepFailedError,
    InvalidSessionIdError,
);

impl InspectProcessError {
    pub(crate) fn for_pid(pid: u32) -> Self {
        Self::new(pid)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fmt::Debug;

    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(NoConsoleError: Send, Sync, Debug, UnwindSafe, RefUnwindSafe);
}
