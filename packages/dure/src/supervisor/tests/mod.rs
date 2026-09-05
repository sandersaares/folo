//! Supervisor tests, grouped by the concern each one is about.

mod attach;
mod preamble;
mod session;
mod startup;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;

use testing::{WatchdogPhaseReporter, with_watchdog_phases};

use super::*;
use crate::constants::{MAX_CLIENT_BACKLOG_BYTES, MAX_OUTPUT_CHUNK_BYTES};
use crate::durability::LauncherTie;
use crate::outbox::Outbox;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{AppId, ConnId, JobId, ListenerId};
use crate::pal::processes::MockProcesses;
use crate::pal::pseudoconsole::{MemoryPseudoconsole, Pseudoconsole, WindowSize};
use crate::pal::session_store::MemorySessionStore;
use crate::pal::transport::MemoryTransport;
use crate::protocol::{Message, PROTOCOL_VERSION, StartupStep, encode, payload_len_ok};
use crate::session_record::{ProcessIdentity, SessionRecord};
use crate::supervisor::relay::{client_loop, pty_output_loop, store_attached_flag};
use crate::supervisor::shared::{Client, FirstAttach, Shared, preamble_messages};
use crate::supervisor::startup::{DEFAULT_PTY_SIZE, map_startup};
use crate::{BreakawayDeniedError, SessionId, StoreError};

/// A publication time with no structure of its own; the age column has its
/// own tests in `list_fmt`.
const SOME_STARTED_AT_MS: u64 = 1;

/// An ordinary session to run, for tests about something other than what
/// the session happens to be.
fn sample_spec() -> SessionSpec {
    SessionSpec {
        launch_directory: PathBuf::from("/work"),
        command: AppCommand::for_test(&["app.exe"]),
        started_at_unix_ms: SOME_STARTED_AT_MS,
    }
}

/// Arbitrary nonzero status the mock app exits with, so a test can tell a
/// forwarded status from a defaulted one.
const SAMPLE_APP_EXIT: i32 = 7;

/// Ordinary valid geometry for tests where resize behavior is out of scope.
const ORDINARY_ATTACH: Message = Message::Attach {
    size: WindowSize::new(80, 24).expect("a fixture size is not empty"),
};

/// What the supervisor reported when it came up.
///
/// Tests take the session pipe from here rather than rebuilding it from the
/// nonce the mock happens to return, so a change to that fixture cannot break
/// a test about something else.
struct StartedSession {
    startup_conn: ConnId,
    session_id: SessionId,
    launcher_tie: LauncherTie,
    pipe_name: String,
}

/// Completes the client side of the startup commit handshake.
fn commit_startup(
    transport: &MemoryTransport,
    listener: ListenerId,
    phase_reporter: &WatchdogPhaseReporter,
) -> StartedSession {
    phase_reporter.report("waiting for the supervisor startup connection");
    let conn = transport.accept(listener).unwrap();
    phase_reporter.report("waiting for the supervisor startup response");
    let Message::StartupOk {
        session_id,
        launcher_tie,
        pipe_name,
    } = transport.recv(conn).unwrap()
    else {
        panic!("expected startup ok");
    };
    transport.send(conn, &Message::StartupCommit).unwrap();
    StartedSession {
        startup_conn: conn,
        session_id,
        launcher_tie,
        pipe_name,
    }
}

fn mock_processes(exit: Arc<(Mutex<bool>, Condvar)>) -> MockProcesses {
    mock_processes_with(exit, LauncherTie::NoneDetected, AppWait::Reports)
}

/// How the mock app's wait ends once the test releases it.
#[derive(Clone, Copy)]
enum AppWait {
    /// The app exited and its status is known.
    Reports,
    /// The wait itself failed, so no status exists to report.
    Fails,
}

fn mock_processes_with(
    exit: Arc<(Mutex<bool>, Condvar)>,
    launcher_tie: LauncherTie,
    wait: AppWait,
) -> MockProcesses {
    mock_processes_with_close_job(exit, launcher_tie, wait, |_| {})
}

fn mock_processes_with_close_job(
    exit: Arc<(Mutex<bool>, Condvar)>,
    launcher_tie: LauncherTie,
    wait: AppWait,
    close_job: impl Fn(JobId) + Send + Sync + 'static,
) -> MockProcesses {
    let mut processes = MockProcesses::new();
    processes
        .expect_launcher_tie()
        .returning(move || launcher_tie);
    processes
        .expect_create_lifetime_job()
        .returning(|| Ok(JobId::for_test(1)));
    processes
        .expect_spawn_app()
        .returning(|_| Ok(AppId::for_test(1)));
    processes.expect_current_identity().returning(|| {
        Ok(ProcessIdentity {
            pid: 10,
            creation_time: 100,
        })
    });
    processes
        .expect_random_nonce()
        .returning(|| "nonce".to_string());
    processes.expect_close_job().returning(close_job);
    processes.expect_wait_app().returning(move |_| {
        let (lock, cvar) = &*exit;
        let mut done = lock.lock().expect("exit lock");
        while !*done {
            done = cvar.wait(done).expect("exit wait");
        }
        match wait {
            AppWait::Reports => Ok(SAMPLE_APP_EXIT),
            AppWait::Fails => Err(PalError::new(PalErrorKind::Other)),
        }
    });
    processes
}

/// Session with a live pty, no client attached.
fn shared_session(
    transport: &MemoryTransport,
    pty_host: &MemoryPseudoconsole,
) -> Shared<MemoryTransport, MemoryPseudoconsole> {
    Shared {
        transport: transport.clone(),
        pty_host: pty_host.clone(),
        pty: pty_host.create(DEFAULT_PTY_SIZE).unwrap(),
        session_id: SessionId::from_u32(1).unwrap(),
        client: Mutex::new(None),
        attach: Mutex::new(()),
        attached_generation: Arc::new(AtomicU64::default()),
        preamble: Mutex::new(Some(Vec::new())),
        first_attach: Mutex::new(FirstAttach::default()),
        first_attach_changed: Condvar::new(),
        stopping: AtomicBool::new(false),
    }
}

/// Connection the client slot currently holds, if any.
fn client_conn(shared: &Shared<MemoryTransport, MemoryPseudoconsole>) -> Option<ConnId> {
    shared.client().as_ref().map(|client| client.conn)
}

/// Connected pair as `(supervisor side, client side)`.
fn connected_pair(transport: &MemoryTransport, name: &str) -> (ConnId, ConnId) {
    let listener = transport.listen(name).unwrap();
    let client = transport.connect(name, CONNECT_TIMEOUT).unwrap();
    let supervisor = transport.accept(listener).unwrap();
    (supervisor, client)
}

/// Records every attached-flag publication in order.
fn attach_recorder() -> (Arc<Mutex<Vec<bool>>>, impl Fn(u64, bool)) {
    let flags = Arc::new(Mutex::new(Vec::new()));
    let recorder = {
        let flags = Arc::clone(&flags);
        move |_generation: u64, attached: bool| {
            flags.lock().expect("flag lock").push(attached);
        }
    };
    (flags, recorder)
}
