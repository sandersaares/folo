//! `dure resume`.

use ohno::AppError;

use crate::attach::attach;
use crate::detect::{DetectOutcome, auto_detect};
use crate::gc::{live_sessions, require_live_session};
use crate::list_fmt::format_list;
use crate::output::{note_line, print_prompt};
use crate::pal::local_console::LocalConsole;
use crate::pal::processes::Processes;
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::session_record::SessionRecord;
use crate::trace::{Trace, trace};
use crate::{
    CanonicalizeError, CurrentDirectoryError, InvalidSessionIdError, NoConsoleError,
    NoLiveSessionsError, Outcome,
    OutputFailedError, PromptFailedError, SessionId,
};

/// Attach using auto-detect or an explicit id.
pub(crate) fn execute<S, P, T, C>(
    store: &S,
    processes: &P,
    transport: &T,
    console: &C,
    id: Option<SessionId>,
    now_unix_ms: u64,
    trace: Trace,
) -> Result<Outcome, AppError>
where
    S: SessionStore,
    P: Processes,
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    // Checked before any selection work: everything below — listing the
    // candidates, prompting for one, reading the record — is wasted on a
    // process that cannot attach whatever it picks.
    if !console.has_console() {
        return Err(NoConsoleError::new().into());
    }
    let id = match id {
        Some(id) => {
            trace!(
                trace,
                "session {id} was named on the command line, so auto-detect is skipped"
            );
            id
        }
        None => resolve_resume_target(store, console, processes, now_unix_ms, trace)?,
    };
    // Read afresh even when the id came from the list printed a moment ago:
    // selection can block on the user, and an id is reusable once its session
    // ends (design.md, "Session identity").
    let record = require_live_session(store, processes, id, trace)?;
    trace!(
        trace,
        "attaching to session {} on {}",
        record.session_id(),
        record.pipe_name
    );
    // Said before the console is taken over, because a failure from here on
    // still leaves this session reachable by `list`, `resume`, and `kill`.
    note_line(format_args!("session {}", record.session_id()));
    attach(transport, console, &record.pipe_name, record.session_id())
}

/// Chooses which live session to resume, asking the user when it has to.
///
/// Auto-detect answers whenever exactly one live session was launched from the
/// current directory. Otherwise this prints the candidates and blocks reading a
/// session id from the terminal.
fn resolve_resume_target<S, C, P>(
    store: &S,
    console: &C,
    processes: &P,
    now_unix_ms: u64,
    trace: Trace,
) -> Result<SessionId, AppError>
where
    S: SessionStore,
    C: LocalConsole,
    P: Processes,
{
    let live = live_sessions(store, processes, trace)?;
    let cwd = store
        .current_dir()
        .map_err(CurrentDirectoryError::caused_by)?;
    let cwd = store
        .canonicalize(&cwd)
        .map_err(|_error| CanonicalizeError::new(cwd))?;
    match auto_detect(&live, &cwd, trace) {
        DetectOutcome::None => Err(NoLiveSessionsError::new().into()),
        DetectOutcome::Unique(id) => Ok(id),
        DetectOutcome::NeedsSelection => prompt_for_session(console, &live, now_unix_ms),
    }
}

/// Prints the candidates and reads the id the user picks.
///
/// This is interactive UI rather than command output, so it goes to stderr:
/// stdout belongs to `dure list`, and a user who redirected it would otherwise
/// wait at a prompt they cannot see.
fn prompt_for_session<C>(
    console: &C,
    live: &[SessionRecord],
    now_unix_ms: u64,
) -> Result<SessionId, AppError>
where
    C: LocalConsole,
{
    if !console.stdin_is_terminal() {
        return Err(PromptFailedError::new().into());
    }
    note_line(format_args!("{}", format_list(live, now_unix_ms)));
    // The read below blocks, so say what is being waited for. A prompt the user
    // cannot see is not worth blocking a read on, so a stream that refuses it
    // fails the command instead.
    print_prompt(format_args!("Session id to resume: ")).map_err(OutputFailedError::caused_by)?;
    let line = console
        .read_prompt_line()
        .map_err(PromptFailedError::caused_by)?;
    parse_prompted_id(&line).map_err(AppError::from)
}

/// Parses a decimal session id from a prompt line.
fn parse_prompted_id(line: &str) -> Result<SessionId, InvalidSessionIdError> {
    let line = line.trim();
    let id: u32 = line
        .parse()
        .map_err(|_error| InvalidSessionIdError::new())?;
    SessionId::from_u32(id).ok_or_else(InvalidSessionIdError::new)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;

    use super::*;
    use crate::AppCommand;

    /// A reading of the clock with no structure of its own; the age column has
    /// its own tests in `list_fmt`.
    const SOME_NOW_MS: u64 = 60_000;
    use crate::pal::error::{PalError, PalErrorKind};
    use crate::pal::ids::RelayLeaseId;
    use crate::pal::local_console::{LocalConsoleFacade, MockLocalConsole};
    use crate::pal::processes::{MockProcesses, ProcessLiveness};
    use crate::pal::pseudoconsole::WindowSize;
    use crate::pal::session_store::{FsSessionStore, SessionStore};
    use crate::pal::transport::MemoryTransport;
    use crate::protocol::Message;
    use crate::session_record::ProcessIdentity;
    use crate::{InvalidSessionIdError, PromptFailedError, SessionId};

    /// Publishes two sessions whose launch directories never match the current
    /// directory, so auto-detect reports an ambiguous result.
    fn publish_ambiguous_sessions(store: &FsSessionStore) {
        for name in ["one", "two"] {
            let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
            store
                .publish(&SessionRecord {
                    id: id.get(),
                    supervisor_pid: 10,
                    supervisor_creation_time: 100,
                    pipe_name: name.to_string(),
                    launch_directory: PathBuf::from(format!("/nowhere/{name}")),
                    command: AppCommand::for_test(&["app.exe"]),
                    started_at_unix_ms: 1,
                    attached: false,
                })
                .unwrap();
        }
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn no_live_sessions_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let processes = MockProcesses::new();
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);
        execute(
            &store,
            &processes,
            &transport,
            &console,
            None,
            SOME_NOW_MS,
            Trace::default(),
        )
        .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn missing_explicit_id_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let processes = MockProcesses::new();
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        let console = LocalConsoleFacade::from_mock(console);
        let id = SessionId::from_u32(9).unwrap();
        execute(
            &store,
            &processes,
            &transport,
            &console,
            Some(id),
            SOME_NOW_MS,
            Trace::default(),
        )
        .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory and
    // the relay threads' blocking recv is guarded by a watchdog thread.
    #[cfg_attr(miri, ignore)]
    fn unique_launch_directory_attaches() {
        testing::with_watchdog(|| {
            let dir = tempfile::TempDir::new().unwrap();
            let store = FsSessionStore::new(dir.path().to_path_buf());
            let cwd = store.current_dir().unwrap();
            let launch_directory = store.canonicalize(&cwd).unwrap();
            let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
            let pipe = "resume-unique";
            store
                .publish(&SessionRecord {
                    id: id.get(),
                    supervisor_pid: 10,
                    supervisor_creation_time: 100,
                    pipe_name: pipe.to_string(),
                    launch_directory,
                    command: AppCommand::for_test(&["app.exe"]),
                    started_at_unix_ms: 1,
                    attached: false,
                })
                .unwrap();
            let mut processes = MockProcesses::new();
            processes
                .expect_probe()
                .returning(|_| ProcessLiveness::Live);
            let transport = MemoryTransport::new();
            let listener = transport.listen(pipe).unwrap();
            thread::spawn({
                let transport = transport.clone();
                move || {
                    let conn = transport.accept(listener).unwrap();
                    _ = transport.recv(conn);
                    _ = transport.send(conn, &Message::Attached { session_id: id });
                    _ = transport.send(conn, &Message::AppExited { status: 0 });
                }
            });
            let mut console = MockLocalConsole::new();
            console.expect_has_console().return_const(true);
            console
                .expect_begin_raw_relay()
                .returning(|| Ok(RelayLeaseId::for_test(1)));
            console.expect_end_raw_relay().returning(|_| Ok(()));
            console
                .expect_window_size()
                .returning(|| Ok(WindowSize { cols: 80, rows: 24 }));
            let reader_cancelled = Arc::new((Mutex::new(false), Condvar::new()));
            console.expect_read_input().returning({
                let reader_cancelled = Arc::clone(&reader_cancelled);
                move || {
                    let (cancelled, changed) = &*reader_cancelled;
                    let mut cancelled = cancelled.lock().unwrap();
                    while !*cancelled {
                        cancelled = changed.wait(cancelled).unwrap();
                    }
                    Err(PalError::new(PalErrorKind::Disconnected))
                }
            });
            console.expect_cancel_input().returning({
                let reader_cancelled = Arc::clone(&reader_cancelled);
                move || {
                    let (cancelled, changed) = &*reader_cancelled;
                    *cancelled.lock().unwrap() = true;
                    changed.notify_all();
                    Ok(())
                }
            });
            console.expect_write_output().returning(|_| Ok(()));
            let console = LocalConsoleFacade::from_mock(console);
            let outcome = execute(
                &store,
                &processes,
                &transport,
                &console,
                None,
                SOME_NOW_MS,
                Trace::default(),
            )
            .unwrap();
            assert!(matches!(outcome, Outcome::AppExit(0)));
        });
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn ambiguous_without_terminal_does_not_prompt() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        publish_ambiguous_sessions(&store);
        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        console.expect_stdin_is_terminal().return_const(false);
        let console = LocalConsoleFacade::from_mock(console);
        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            None,
            SOME_NOW_MS,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<PromptFailedError>().is_some());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn ambiguous_with_terminal_reads_selection() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        publish_ambiguous_sessions(&store);
        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);
        let transport = MemoryTransport::new();
        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(true);
        console.expect_stdin_is_terminal().return_const(true);
        console
            .expect_read_prompt_line()
            .returning(|| Ok("not a number".to_string()));
        let console = LocalConsoleFacade::from_mock(console);
        let error = execute(
            &store,
            &processes,
            &transport,
            &console,
            None,
            SOME_NOW_MS,
            Trace::default(),
        )
        .unwrap_err();
        assert!(error.find_source::<InvalidSessionIdError>().is_some());
    }
}
