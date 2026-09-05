//! The parsed invocation the library entry point executes.

use std::path::PathBuf;

use crate::app_command::AppCommand;
use crate::session_id::SessionId;

/// Parsed command the binary should execute.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Command {
    /// Start a new session in the current directory and attach to it.
    ///
    /// The command is executed directly rather than through a shell, so no
    /// quoting, globbing, or operator is interpreted. A relative path is
    /// resolved against the current directory and a bare name against the
    /// executable search path.
    Run {
        /// Executable and arguments the session runs.
        command: AppCommand,
    },
    /// Attach to an existing live session.
    Resume {
        /// Explicit session id.
        ///
        /// `None` auto-detects: a single live session launched from the current
        /// directory is taken, and anything else lists the live sessions and
        /// reads an id from the terminal, which fails when there is no terminal
        /// to read from.
        id: Option<SessionId>,
    },
    /// Print live sessions.
    List,
    /// Abruptly terminate the recorded supervisor for this id.
    ///
    /// The app and its ordinary descendants die with the supervisor.
    Kill {
        /// Session id to kill. Auto-detect is deliberately not applied here.
        id: SessionId,
    },
    /// Supervisor process spawned by `run`. Not a user-facing subcommand.
    #[doc(hidden)]
    Supervisor {
        /// One-shot startup pipe name created by the client.
        startup_pipe: String,
        /// Canonical launch directory.
        launch_directory: PathBuf,
        /// Executable and arguments the session runs.
        command: AppCommand,
    },
}

/// One fully parsed `dure` invocation.
///
/// Built by [`crate::Cli::into_invocation`] and consumed by [`crate::run`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::exhaustive_structs,
    reason = "handoff struct read directly by the in-crate binary"
)]
pub struct Invocation {
    /// Whether the command explains what it inspects and decides, on stderr.
    ///
    /// Applies to every command, not to one of them: auto-detect is the most
    /// visible example, but store location, record reads, and liveness
    /// decisions are explained the same way (design.md, "Diagnostics").
    pub verbose: bool,
    /// Where the session store lives, when it is not the per-user default.
    ///
    /// Compiled only into test builds. Session isolation is a property of the
    /// per-user store root: a directory chosen by a caller carries whatever
    /// access control it happens to have, and records under it are trusted by
    /// `list`, `resume`, and `kill`. The released tool therefore has no way to
    /// point at one (design.md, "Isolation").
    #[cfg(any(test, feature = "private-test-util"))]
    #[doc(hidden)]
    pub store_root: Option<PathBuf>,
    /// Subcommand to execute.
    pub command: Command,
}

impl Invocation {
    /// Where the session store lives, when it is not the per-user default.
    #[must_use]
    pub(crate) fn session_store_root(&self) -> Option<PathBuf> {
        #[cfg(any(test, feature = "private-test-util"))]
        {
            self.store_root.clone()
        }
        #[cfg(not(any(test, feature = "private-test-util")))]
        {
            None
        }
    }
}

/// Result of a successful `dure` invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    clippy::exhaustive_enums,
    reason = "handoff enum matched directly by the in-crate binary"
)]
pub enum Outcome {
    /// Command finished without an app exit status to forward.
    Success,
    /// The attached app exited; the process should exit with this status.
    AppExit(i32),
}
