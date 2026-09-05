//! Supervisor role: own the app, accept clients, last-attach-wins steal.
//!
//! The session as a running thing is three concerns, one per module below:
//! bringing it into existence, the state its threads coordinate through, and
//! the relay that carries the console until the app exits.
//! Ref: docs/supervisor.md.

mod relay;
mod shared;
mod startup;

use std::path::PathBuf;

use ohno::AppError;

use crate::constants::CONNECT_TIMEOUT;
use crate::pal::processes::Processes;
use crate::pal::pseudoconsole::Pseudoconsole;
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::protocol::Message;
use crate::supervisor::relay::serve;
use crate::supervisor::startup::{InitGuard, initialize};
use crate::{AppCommand, StartupFailedError};

/// What the supervisor is being asked to run.
///
/// One value rather than three parameters because these travel together and
/// mean nothing apart: they are the session, as distinct from the platform it
/// runs on.
pub(crate) struct SessionSpec {
    /// Canonical directory the app is started in, and the key auto-detect
    /// matches a later `resume` against.
    pub launch_directory: PathBuf,
    /// Executable and arguments the session runs.
    pub command: AppCommand,
    /// When the session was published, for the age `list` renders.
    pub started_at_unix_ms: u64,
}

/// Initialize the session, publish the record, then relay until the app exits.
// Blocking supervisor entry point. A mutation that returns before serving leaves
// the test's client waiting on a session that never appears, and watchdogs are
// disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn run_supervisor<P, S, T, C>(
    processes: &P,
    store: &S,
    transport: &T,
    pty_host: &C,
    startup_pipe: &str,
    spec: SessionSpec,
) -> Result<i32, AppError>
where
    P: Processes,
    S: SessionStore + Clone,
    T: Transport + Clone + Send + Sync + 'static,
    C: Pseudoconsole + Clone + Send + Sync + 'static,
{
    let startup = transport
        .connect(startup_pipe, CONNECT_TIMEOUT)
        .map_err(StartupFailedError::caused_by)?;

    let mut guard = InitGuard {
        processes,
        store,
        transport,
        pty_host,
        job: None,
        pty: None,
        listener: None,
        session: None,
        committed: false,
    };

    let result = initialize(&mut guard, processes, store, transport, pty_host, spec);

    let initialized = match result {
        Ok(initialized) => initialized,
        Err(error) => {
            _ = transport.send(startup, &Message::StartupErr);
            transport.disconnect(startup);
            return Err(error);
        }
    };

    let startup_ok = Message::StartupOk {
        session_id: initialized.session_id,
        // Only this process can see the job it landed in, and the client is
        // the one with a console to report it on.
        // Ref: docs/job-breakaway.md.
        launcher_tie: processes.launcher_tie(),
    };
    if transport.send(startup, &startup_ok).is_err() {
        transport.disconnect(startup);
        return Err(StartupFailedError::new().into());
    }
    let committed = transport.recv_timeout(startup, CONNECT_TIMEOUT);
    if !matches!(committed, Ok(Message::StartupCommit)) {
        transport.disconnect(startup);
        return Err(StartupFailedError::new().into());
    }
    guard.committed = true;

    // The startup connection stays open past the acknowledgement: `serve` reads
    // it as the initiator's liveness signal and disconnects it.
    let status = serve(processes, store, transport, pty_host, &initialized, startup)?;
    Ok(status)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
