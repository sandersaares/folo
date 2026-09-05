//! Bringing a session into existence, and undoing it if that fails.
//!
//! Ref: docs/supervisor.md, "Startup".

use ohno::AppError;

use crate::constants::{DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS};
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{AppId, JobId, ListenerId, PtyId};
use crate::pal::processes::{AppSpawn, Processes};
use crate::pal::pseudoconsole::{Pseudoconsole, WindowSize};
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::protocol::{PROTOCOL_VERSION, StartupStep};
use crate::session_record::{ProcessIdentity, SessionRecord};
use crate::supervisor::SessionSpec;
use crate::{BreakawayDeniedError, SessionId, StartupFailedError, StoreError};

/// Size used until the first client attaches.
///
/// VGA text-mode geometry (`DEFAULT_PTY_COLS` by `DEFAULT_PTY_ROWS`). The first
/// attach always resizes to the client's real size (design.md, "Terminal
/// pass-through").
pub(super) const DEFAULT_PTY_SIZE: WindowSize = WindowSize {
    cols: DEFAULT_PTY_COLS,
    rows: DEFAULT_PTY_ROWS,
};
/// Resources that must be torn down if initialization fails.
pub(super) struct InitGuard<'a, P: Processes, S: SessionStore, T: Transport, C: Pseudoconsole> {
    pub(super) processes: &'a P,
    pub(super) store: &'a S,
    pub(super) transport: &'a T,
    pub(super) pty_host: &'a C,
    pub(super) job: Option<JobId>,
    pub(super) pty: Option<PtyId>,
    pub(super) listener: Option<ListenerId>,
    pub(super) session: Option<(SessionId, ProcessIdentity)>,
    pub(super) committed: bool,
}

impl<P: Processes, S: SessionStore, T: Transport, C: Pseudoconsole> Drop
    for InitGuard<'_, P, S, T, C>
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(listener) = self.listener {
            self.transport.close_listener(listener);
        }
        if let Some((id, owner)) = self.session {
            _ = self.store.delete_owned_by(id, &owner);
        }
        // Descendants of the app stay attached to the pseudoconsole until the
        // job that owns their lifetime is closed, and closing a pseudoconsole
        // waits for its attached clients.
        if let Some(job) = self.job {
            self.processes.close_job(job);
        }
        if let Some(pty) = self.pty {
            self.pty_host.close(pty);
        }
    }
}

pub(super) struct Initialized {
    pub(super) session_id: SessionId,
    pub(super) identity: ProcessIdentity,
    pub(super) listener: ListenerId,
    pub(super) pty: PtyId,
    pub(super) job: JobId,
    pub(super) app: AppId,
    /// The pipe clients attach on, so the initiating client can be told it
    /// rather than reading back the record just written.
    pub(super) pipe_name: String,
}

/// What `initialize` failed at.
///
/// The supervisor has no console of its own, so the step is the only thing
/// that tells the user which subsystem to look at.
/// Ref: docs/supervisor.md, "Startup".
pub(super) struct FailedStartup {
    pub(super) step: StartupStep,
    pub(super) error: AppError,
}

pub(super) fn initialize<P, S, T, C>(
    guard: &mut InitGuard<'_, P, S, T, C>,
    processes: &P,
    store: &S,
    transport: &T,
    pty_host: &C,
    spec: SessionSpec,
) -> Result<Initialized, FailedStartup>
where
    P: Processes,
    S: SessionStore,
    T: Transport,
    C: Pseudoconsole,
{
    let at = |step: StartupStep| {
        move |error: PalError| FailedStartup {
            step,
            error: map_startup(&error),
        }
    };
    let storing = |step: StartupStep| {
        move |error: PalError| FailedStartup {
            step,
            error: StoreError::caused_by(error).into(),
        }
    };

    let job = processes
        .create_lifetime_job()
        .map_err(at(StartupStep::LifetimeJob))?;
    guard.job = Some(job);

    let pty = pty_host
        .create(DEFAULT_PTY_SIZE)
        .map_err(at(StartupStep::Pseudoconsole))?;
    guard.pty = Some(pty);

    let app = processes
        .spawn_app(&AppSpawn {
            command: spec.command.clone(),
            launch_directory: spec.launch_directory.clone(),
            pty,
            job,
        })
        .map_err(at(StartupStep::App))?;

    let nonce = processes.random_nonce();
    let pipe_name = transport.pipe_name(&nonce);
    let listener = transport
        .listen(&pipe_name)
        .map_err(at(StartupStep::Listener))?;
    guard.listener = Some(listener);

    let identity = processes
        .current_identity()
        .map_err(at(StartupStep::Identity))?;
    let session_id = store
        .allocate_id(&identity)
        .map_err(storing(StartupStep::SessionId))?;
    guard.session = Some((session_id, identity));

    let record = SessionRecord {
        id: session_id,
        supervisor: identity,
        pipe_name: pipe_name.clone(),
        launch_directory: spec.launch_directory,
        command: spec.command,
        started_at_unix_ms: spec.started_at_unix_ms,
        attached: false,
        protocol_version: PROTOCOL_VERSION,
    };
    store
        .publish(&record)
        .map_err(storing(StartupStep::PublishRecord))?;

    Ok(Initialized {
        session_id,
        identity,
        listener,
        pty,
        job,
        app,
        pipe_name,
    })
}

pub(super) fn map_startup(error: &PalError) -> AppError {
    match error.kind() {
        PalErrorKind::BreakawayDenied => BreakawayDeniedError::new().into(),
        _ => StartupFailedError::new().into(),
    }
}
