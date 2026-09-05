//! Hidden supervisor process.

use ohno::AppError;

use crate::Outcome;
use crate::pal::processes::Processes;
use crate::pal::pseudoconsole::Pseudoconsole;
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::supervisor::{SessionSpec, run_supervisor};

/// Run the supervisor role until the app exits.
pub(crate) fn execute<S, P, T, Y>(
    store: &S,
    processes: &P,
    transport: &T,
    pty_host: &Y,
    startup_pipe: &str,
    spec: SessionSpec,
) -> Result<Outcome, AppError>
where
    S: SessionStore + Clone,
    P: Processes,
    T: Transport + Clone + Send + Sync + 'static,
    Y: Pseudoconsole + Clone + Send + Sync + 'static,
{
    let status = run_supervisor(processes, store, transport, pty_host, startup_pipe, spec)?;
    Ok(Outcome::AppExit(status))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::pal::processes::MockProcesses;
    use crate::pal::pseudoconsole::MemoryPseudoconsole;
    use crate::pal::session_store::{MockSessionStore, SessionStoreFacade};
    use crate::pal::transport::MemoryTransport;
    use crate::{AppCommand, StartupFailedError};

    /// A publication time with no structure of its own; the age column has its
    /// own tests in `list_fmt`.
    const SOME_STARTED_AT_MS: u64 = 1;

    #[test]
    fn a_supervisor_that_cannot_report_in_fails() {
        // Nothing is listening on the startup pipe, so the supervisor never
        // reaches the point of owning an app whose status it could forward.
        let error = execute(
            &SessionStoreFacade::from_mock(MockSessionStore::new()),
            &MockProcesses::new(),
            &MemoryTransport::new(),
            &MemoryPseudoconsole::new(),
            "missing",
            SessionSpec {
                launch_directory: PathBuf::from("/work"),
                command: AppCommand::from_argv(vec!["app.exe".to_string()]).unwrap(),
                started_at_unix_ms: SOME_STARTED_AT_MS,
            },
        )
        .unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
    }
}
