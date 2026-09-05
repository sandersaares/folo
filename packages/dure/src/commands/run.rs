//! `dure run`.

use std::path::PathBuf;
use std::time::Duration;

use ohno::AppError;

use crate::attach::attach;
use crate::constants::{CONNECT_TIMEOUT, STARTUP_TIMEOUT, SUPERVISOR_COMMAND};
use crate::durability::LauncherTie;
use crate::output::note_line;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{ConnId, ListenerId};
use crate::pal::local_console::LocalConsole;
use crate::pal::processes::{HowResolved, Processes, SupervisorSpawn};
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::path_display::display_path;
use crate::protocol::Message;
use crate::trace::{Trace, trace};
use crate::{
    AppCommand, BreakawayDeniedError, CanonicalizeError, CurrentDirectoryError, NoConsoleError,
    Outcome, PalFailedError, StartupFailedError, StartupStepFailedError,
};

/// Said when the supervisor confirmed a job that ends the session with its
/// launcher.
///
/// Ref: docs/job-breakaway.md.
const TIED_TO_LAUNCHER_WARNING: &str = concat!(
    "Warning: this session belongs to a Windows job object that will end it when the launcher ",
    "exits, so it will not survive a disconnect. Launch dure.exe directly instead of through a ",
    "wrapper such as `cargo run`."
);

/// Said when the supervisor could not inspect the job it is in.
///
/// Nothing was established either way, so this reports the uncertainty rather
/// than naming a cause that was never confirmed.
/// Ref: docs/job-breakaway.md.
const UNKNOWN_TIE_WARNING: &str = concat!(
    "Warning: this session's Windows job object could not be inspected, so whether it survives ",
    "the launcher is unknown. Launch dure.exe directly if the session must outlive this terminal."
);

/// Start a new session, spawn the supervisor, and attach.
pub(crate) fn execute<S, P, T, C>(
    store: &S,
    processes: &P,
    transport: &T,
    console: &C,
    command: &AppCommand,
    store_root: Option<PathBuf>,
    trace: Trace,
) -> Result<Outcome, AppError>
where
    S: SessionStore,
    P: Processes,
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    if !console.has_console() {
        return Err(NoConsoleError::new().into());
    }
    trace!(trace, "app to run: {command}");

    let cwd = store
        .current_dir()
        .map_err(CurrentDirectoryError::caused_by)?;
    let launch_directory = store
        .canonicalize(&cwd)
        .map_err(|_error| CanonicalizeError::new(cwd))?;
    // Auto-detect matches on this canonicalized form, so it is what a later
    // `dure resume` in this directory will compare against.
    trace!(
        trace,
        "launch directory: {} (auto-detect will match a resume from here)",
        display_path(&launch_directory)
    );
    if trace.is_enabled() {
        let resolved = processes.resolve_executable(command.exe(), &launch_directory);
        trace!(
            trace,
            "app executable: {} ({})",
            display_path(&resolved.path),
            resolution_note(resolved.how)
        );
    }

    let nonce = processes.random_nonce();
    let startup_pipe = transport.pipe_name(&format!("startup-{nonce}"));
    trace!(
        trace,
        "listening on {startup_pipe} for the supervisor to report in"
    );
    // Held in a guard so an unwind from anything below closes the listener and,
    // once accepted, the startup connection. The supervisor reads that
    // connection closing as the client giving up, so leaking it across an
    // unwind would leave a session waiting for an attach that is never coming.
    // Ref: docs/implementation.md, "Process split".
    let mut startup =
        StartupChannel::listen(transport, &startup_pipe).map_err(StartupFailedError::caused_by)?;

    let exe = processes.current_exe().map_err(PalFailedError::caused_by)?;
    let mut args = vec![
        SUPERVISOR_COMMAND.to_string(),
        "--startup-pipe".to_string(),
        startup_pipe,
        "--launch-directory".to_string(),
        launch_directory.to_string_lossy().into_owned(),
    ];
    if let Some(root) = store_root {
        args.push("--store-root".to_string());
        args.push(root.to_string_lossy().into_owned());
    }
    args.push("--".to_string());
    args.extend(command.argv());

    if trace.is_enabled() {
        let mut spawn_line = vec![exe.to_string_lossy().into_owned()];
        spawn_line.extend(args.iter().cloned());
        trace!(
            trace,
            "spawning the supervisor: {}",
            AppCommand::from_argv(spawn_line).map_or_else(String::new, |line| line.to_string())
        );
    }
    processes
        .spawn_supervisor(&SupervisorSpawn { exe, args })
        .map_err(|error| match error.kind() {
            PalErrorKind::BreakawayDenied => AppError::from(BreakawayDeniedError::new()),
            _ => AppError::from(StartupFailedError::new()),
        })?;

    // Initialization gets its own full deadline after this connection is
    // established.
    let conn = startup
        .accept(CONNECT_TIMEOUT)
        .map_err(StartupFailedError::caused_by)?;

    let response = transport.recv_timeout(conn, STARTUP_TIMEOUT);
    let (session_id, launcher_tie, pipe_name) = match response {
        Ok(Message::StartupOk {
            session_id,
            launcher_tie,
            pipe_name,
        }) => (session_id, launcher_tie, pipe_name),
        // The supervisor runs without a console, so this is the only place the
        // failing subsystem can be named. Ref: docs/supervisor.md, "Startup".
        Ok(Message::StartupErr { step }) => return Err(StartupStepFailedError::at(step).into()),
        _ => return Err(StartupFailedError::new().into()),
    };
    if transport.send(conn, &Message::StartupCommit).is_err() {
        return Err(StartupFailedError::new().into());
    }
    trace!(
        trace,
        "supervisor reported in as session {session_id}; launcher tie: {launcher_tie}"
    );
    if launcher_tie.warrants_warning() {
        // The supervisor discovers this about itself but has no console
        // to say it on. Ref: docs/job-breakaway.md.
        note_line(format_args!("{}", launcher_warning(launcher_tie)));
    }
    // Said before the console is taken over, because a failure from here on
    // still leaves this session reachable by `list`, `resume`, and `kill`.
    note_line(format_args!("session {session_id}"));
    trace!(trace, "attaching to session {session_id} on {pipe_name}");
    // The supervisor reads this connection as the signal that an attach is
    // still on its way, and holds a session whose app exits immediately open
    // until it arrives. So it stays up for as long as this run intends to
    // attach. Ref: docs/implementation.md, "Process split".
    attach(transport, console, &pipe_name, session_id)
}

/// The one-shot channel `run` gives the supervisor to report in on.
///
/// Owning it makes closing it unconditional. The supervisor treats this
/// connection closing as the client no longer intending to attach, so an unwind
/// that skipped the close would leave a session waiting forever for a client
/// that has already gone. Ref: docs/implementation.md, "Process split".
struct StartupChannel<'a, T: Transport> {
    transport: &'a T,
    listener: Option<ListenerId>,
    conn: Option<ConnId>,
}

impl<'a, T: Transport> StartupChannel<'a, T> {
    fn listen(transport: &'a T, pipe_name: &str) -> Result<Self, PalError> {
        let listener = transport.listen(pipe_name)?;
        Ok(Self {
            transport,
            listener: Some(listener),
            conn: None,
        })
    }

    /// Accepts the supervisor and stops listening for anyone else.
    fn accept(&mut self, timeout: Duration) -> Result<ConnId, PalError> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
        let accepted = self.transport.accept_timeout(listener, timeout);
        self.transport.close_listener(listener);
        let conn = accepted?;
        self.conn = Some(conn);
        Ok(conn)
    }
}

impl<T: Transport> Drop for StartupChannel<'_, T> {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            self.transport.close_listener(listener);
        }
        if let Some(conn) = self.conn.take() {
            self.transport.disconnect(conn);
        }
    }
}

// Trace wording is not a behavioral contract; the resolution it explains is.
#[cfg_attr(test, mutants::skip)]
fn resolution_note(how: HowResolved) -> &'static str {
    match how {
        HowResolved::Absolute => "an absolute path, taken as written",
        HowResolved::RelativeToLaunchDirectory => "a path, taken relative to the launch directory",
        HowResolved::SearchPath => "a bare name, found on the executable search path",
        HowResolved::NotFound => "a bare name the executable search path does not have",
    }
}

/// What to tell the user about a session that may not outlive its launcher.
///
/// A confirmed tie names the cause; an unreadable job does not, because the
/// supervisor established nothing and saying otherwise would send the user
/// after a diagnosis that was never made.
#[cfg_attr(test, mutants::skip)]
fn launcher_warning(launcher_tie: LauncherTie) -> &'static str {
    match launcher_tie {
        LauncherTie::Confirmed => TIED_TO_LAUNCHER_WARNING,
        LauncherTie::Unknown => UNKNOWN_TIE_WARNING,
        LauncherTie::NoneDetected => "",
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::pal::error::PalError;
    use crate::pal::ids::RelayLeaseId;
    use crate::pal::local_console::{LocalConsoleFacade, MockLocalConsole};
    use crate::pal::processes::MockProcesses;
    use crate::pal::pseudoconsole::WindowSize;
    use crate::pal::session_store::{FsSessionStore, MockSessionStore};
    use crate::pal::transport::MemoryTransport;
    use crate::protocol::StartupStep;
    use crate::session_record::ProcessIdentity;
    use crate::{AttachFailedError, SessionId};

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn no_console_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let processes = MockProcesses::new();
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(false);
        let console = LocalConsoleFacade::from_mock(console);
        execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn breakaway_denied_is_breakaway_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes
            .expect_spawn_supervisor()
            .returning(|_| Err(PalError::new(PalErrorKind::BreakawayDenied)));
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);
        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<BreakawayDeniedError>().is_some());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn spawn_failure_is_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes
            .expect_spawn_supervisor()
            .returning(|_| Err(PalError::new(PalErrorKind::Other)));
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);
        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_does_not_connect_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let transport = MemoryTransport::new();
        transport.expire_next_accept(&transport.pipe_name("startup-nonce"));
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes.expect_spawn_supervisor().returning(|_| {
            Ok(ProcessIdentity {
                pid: 10,
                creation_time: 100,
            })
        });
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);

        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
        // The startup channel is closed on the way out, so nothing is left for
        // a supervisor that turns up late to connect to.
        let error = transport
            .connect(&transport.pipe_name("startup-nonce"), CONNECT_TIMEOUT)
            .unwrap_err();
        assert_eq!(error.kind(), PalErrorKind::NotFound);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_connects_without_reporting_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let transport = MemoryTransport::new();
        transport.expire_next_recv(&transport.pipe_name("startup-nonce"));
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes.expect_spawn_supervisor().returning({
            let transport = transport.clone();
            move |_| {
                let pipe = transport.pipe_name("startup-nonce");
                _ = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
                Ok(ProcessIdentity {
                    pid: 10,
                    creation_time: 100,
                })
            }
        });
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);

        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();

        assert!(error.find_source::<StartupFailedError>().is_some());
        assert_eq!(transport.startup_commit_count(), 0);
        // The startup channel is closed on the way out, so nothing is left for
        // a supervisor that turns up late to connect to.
        let error = transport
            .connect(&transport.pipe_name("startup-nonce"), CONNECT_TIMEOUT)
            .unwrap_err();
        assert_eq!(error.kind(), PalErrorKind::NotFound);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_reports_failure_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let transport = MemoryTransport::new();
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes.expect_spawn_supervisor().returning({
            let transport = transport.clone();
            move |_| {
                let pipe = transport.pipe_name("startup-nonce");
                let conn = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
                transport
                    .send(
                        conn,
                        &Message::StartupErr {
                            step: StartupStep::App,
                        },
                    )
                    .unwrap();
                Ok(ProcessIdentity {
                    pid: 10,
                    creation_time: 100,
                })
            }
        });
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);

        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        // The user is told which subsystem stopped startup, not only that
        // something did.
        assert!(error.to_string().contains(StartupStep::App.describe()));
        assert!(error.find_source::<StartupStepFailedError>().is_some());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_disconnects_after_startup_ok_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let transport = MemoryTransport::new();
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes.expect_spawn_supervisor().returning({
            let transport = transport.clone();
            move |_| {
                let pipe = transport.pipe_name("startup-nonce");
                let conn = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
                transport
                    .send(
                        conn,
                        &Message::StartupOk {
                            session_id: SessionId::MIN,
                            launcher_tie: LauncherTie::NoneDetected,
                            pipe_name: "session-pipe".to_string(),
                        },
                    )
                    .unwrap();
                transport.disconnect(conn);
                Ok(ProcessIdentity {
                    pid: 10,
                    creation_time: 100,
                })
            }
        });
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);

        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
        assert_eq!(transport.startup_commit_count(), 0);
    }

    /// Drives `execute` through a successful startup handshake against a
    /// supervisor stand-in that reports `launcher_tie`, then leaves nobody
    /// listening on the session pipe so the run ends at the attach.
    fn execute_past_startup(launcher_tie: LauncherTie) -> AppError {
        let transport = MemoryTransport::new();
        let mut store = MockSessionStore::new();
        store
            .expect_current_dir()
            .returning(|| Ok(PathBuf::from("cwd")));
        store
            .expect_canonicalize()
            .returning(|path| Ok(path.to_path_buf()));
        let mut processes = MockProcesses::new();
        processes
            .expect_random_nonce()
            .returning(|| "nonce".to_string());
        processes
            .expect_current_exe()
            .returning(|| Ok(PathBuf::from("dure.exe")));
        processes.expect_spawn_supervisor().returning({
            let transport = transport.clone();
            move |_| {
                let pipe = transport.pipe_name("startup-nonce");
                let conn = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
                transport
                    .send(
                        conn,
                        &Message::StartupOk {
                            session_id: SessionId::MIN,
                            launcher_tie,
                            pipe_name: "session-pipe".to_string(),
                        },
                    )
                    .unwrap();
                Ok(ProcessIdentity {
                    pid: 10,
                    creation_time: 100,
                })
            }
        });
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        // Attach takes the console over before it reaches the pipe, and hands
        // it back on the way out of the failure.
        console
            .expect_begin_raw_relay()
            .returning(|| Ok(RelayLeaseId::for_test(1)));
        console.expect_end_raw_relay().returning(|_| Ok(()));
        console
            .expect_window_size()
            .returning(|| Ok(WindowSize::new(80, 24).expect("a fixture size is not empty")));
        let console = LocalConsoleFacade::from_mock(console);

        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            &AppCommand::for_test(&["app.exe"]),
            None,
            Trace::default(),
        )
        .unwrap_err();
        assert_eq!(transport.startup_commit_count(), 1);
        error
    }

    #[test]
    fn a_started_session_is_attached_on_the_pipe_the_supervisor_named() {
        let error = execute_past_startup(LauncherTie::NoneDetected);
        assert!(error.find_source::<AttachFailedError>().is_some());
    }

    #[test]
    fn a_session_tied_to_the_launcher_still_starts() {
        let error = execute_past_startup(LauncherTie::Confirmed);
        assert!(error.find_source::<AttachFailedError>().is_some());
    }

    #[test]
    fn a_session_whose_job_could_not_be_inspected_still_starts() {
        let error = execute_past_startup(LauncherTie::Unknown);
        assert!(error.find_source::<AttachFailedError>().is_some());
    }

    #[test]
    fn only_an_established_or_unknown_tie_is_worth_warning_about() {
        assert!(LauncherTie::Confirmed.warrants_warning());
        assert!(LauncherTie::Unknown.warrants_warning());
        assert!(!LauncherTie::NoneDetected.warrants_warning());
        // An unreadable job must not be reported as a confirmed diagnosis.
        assert_ne!(
            launcher_warning(LauncherTie::Unknown),
            launcher_warning(LauncherTie::Confirmed)
        );
    }
}
