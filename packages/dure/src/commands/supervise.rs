//! Hidden supervisor process.

use std::path::PathBuf;

use ohno::AppError;

use crate::AppCommand;
use crate::Outcome;
use crate::pal::processes::Processes;
use crate::pal::pseudoconsole::Pseudoconsole;
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::supervisor::run_supervisor;

/// Run the supervisor role until the app exits.
pub(crate) fn execute<S, P, T, Y>(
    store: &S,
    processes: &P,
    transport: &T,
    pty_host: &Y,
    startup_pipe: &str,
    launch_directory: PathBuf,
    command: AppCommand,
) -> Result<Outcome, AppError>
where
    S: SessionStore + Clone,
    P: Processes,
    T: Transport + Clone + Send + Sync + 'static,
    Y: Pseudoconsole + Clone + Send + Sync + 'static,
{
    let status = run_supervisor(
        processes,
        store,
        transport,
        pty_host,
        startup_pipe,
        launch_directory,
        command,
    )?;
    Ok(Outcome::AppExit(status))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::StartupFailedError;
    use crate::pal::processes::MockProcesses;
    use crate::pal::pseudoconsole::MemoryPseudoconsole;
    use crate::pal::session_store::{MockSessionStore, SessionStoreFacade};
    use crate::pal::transport::MemoryTransport;

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
            PathBuf::from("/work"),
            AppCommand::from_argv(vec!["app.exe".to_string()]).unwrap(),
        )
        .unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
    }
}
