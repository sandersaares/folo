//! Supervisor role: own the app, accept clients, last-attach-wins steal.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

use ohno::AppError;

use crate::constants::{
    CONNECT_TIMEOUT, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, MAX_CLIENT_BACKLOG_BYTES,
    MAX_OUTPUT_CHUNK_BYTES,
};
use crate::outbox::Outbox;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{AppId, ConnId, JobId, ListenerId, PtyId};
use crate::pal::processes::{AppSpawn, Processes};
use crate::pal::pseudoconsole::{Pseudoconsole, WindowSize};
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::protocol::Message;
use crate::session_record::{ProcessIdentity, SessionRecord};
use crate::wall_clock::unix_now_ms;
use crate::{
    AppCommand, BreakawayDeniedError, PalFailedError, SessionId, StartupFailedError, StoreError,
};

/// Size used until the first client attaches.
///
/// VGA text-mode geometry (`DEFAULT_PTY_COLS` by `DEFAULT_PTY_ROWS`). The first
/// attach always resizes to the client's real size (design.md, "Terminal
/// pass-through").
const DEFAULT_PTY_SIZE: WindowSize = WindowSize {
    cols: DEFAULT_PTY_COLS,
    rows: DEFAULT_PTY_ROWS,
};

/// Resources that must be torn down if initialization fails.
struct InitGuard<'a, P: Processes, S: SessionStore, T: Transport, C: Pseudoconsole> {
    processes: &'a P,
    store: &'a S,
    transport: &'a T,
    pty_host: &'a C,
    job: Option<JobId>,
    pty: Option<PtyId>,
    listener: Option<ListenerId>,
    session: Option<(SessionId, ProcessIdentity)>,
    committed: bool,
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
    launch_directory: PathBuf,
    command: AppCommand,
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

    let result = initialize(
        &mut guard,
        processes,
        store,
        transport,
        pty_host,
        launch_directory,
        command,
    );

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

#[derive(Clone, Copy)]
struct Initialized {
    session_id: SessionId,
    identity: ProcessIdentity,
    listener: ListenerId,
    pty: PtyId,
    job: JobId,
    app: AppId,
}

fn initialize<P, S, T, C>(
    guard: &mut InitGuard<'_, P, S, T, C>,
    processes: &P,
    store: &S,
    transport: &T,
    pty_host: &C,
    launch_directory: PathBuf,
    command: AppCommand,
) -> Result<Initialized, AppError>
where
    P: Processes,
    S: SessionStore,
    T: Transport,
    C: Pseudoconsole,
{
    let job = processes
        .create_lifetime_job()
        .map_err(|error| map_startup(&error))?;
    guard.job = Some(job);

    let pty = pty_host
        .create(DEFAULT_PTY_SIZE)
        .map_err(|error| map_startup(&error))?;
    guard.pty = Some(pty);

    let app = processes
        .spawn_app(&AppSpawn {
            command: command.clone(),
            launch_directory: launch_directory.clone(),
            pty,
            job,
        })
        .map_err(|error| map_startup(&error))?;

    let nonce = processes.random_nonce();
    let pipe_name = transport.pipe_name(&nonce);
    let listener = transport
        .listen(&pipe_name)
        .map_err(|error| map_startup(&error))?;
    guard.listener = Some(listener);

    let identity = processes
        .current_identity()
        .map_err(|error| map_startup(&error))?;
    let session_id = store
        .allocate_id(&identity)
        .map_err(StoreError::caused_by)?;
    guard.session = Some((session_id, identity));

    let record = SessionRecord {
        id: session_id.get(),
        supervisor_pid: identity.pid,
        supervisor_creation_time: identity.creation_time,
        pipe_name,
        launch_directory,
        command,
        started_at_unix_ms: unix_now_ms(),
        attached: false,
    };
    store.publish(&record).map_err(StoreError::caused_by)?;

    Ok(Initialized {
        session_id,
        identity,
        listener,
        pty,
        job,
        app,
    })
}

fn map_startup(error: &PalError) -> AppError {
    match error.kind() {
        PalErrorKind::BreakawayDenied => BreakawayDeniedError::new().into(),
        _ => StartupFailedError::new().into(),
    }
}

/// Everything the supervisor's threads coordinate through.
///
/// A live session runs four things at once: an accept loop waiting for the next
/// client, a relay reading whichever client currently owns the console, a pump
/// reading the app's output, and a wait on the app itself. They meet only here,
/// and the fields below are the whole of what they share.
///
/// Three questions are decided in this struct, each with its own lock, and they
/// are separate because they are answered at different moments:
///
/// * **Who owns the console** — `client`, serialized by `attach`. An attach is
///   one transaction: acknowledge, install, displace. `attached_generation`
///   names each successive answer so a slower observer cannot publish a stale
///   one.
/// * **Whether anyone has come for the session yet** — `first_attach`. An app
///   that exits immediately must not be torn down before the client that
///   started it arrives.
/// * **What the app has written** — `preamble` for the output it produced before
///   the first attach, and `stopping` for the point after which nothing more
///   will be relayed.
///
/// Ref: docs/supervisor.md.
struct Shared<T: Transport, C> {
    transport: T,
    pty_host: C,
    pty: PtyId,
    session_id: SessionId,
    /// Holding this across a pseudoconsole write keeps a displaced client from
    /// reaching the app between its ownership check and the write itself.
    /// Writes here land in the console host's input buffer, which the host
    /// drains independently of the app, so the hold is bounded. Attach also
    /// acknowledges under it, which orders `Attached` ahead of any `Output` on
    /// the same connection and closes the window in which output would be
    /// discarded for want of an installed client.
    client: Mutex<Option<Client<T>>>,
    /// Serializes an entire attach: acknowledgement, ownership transfer, and
    /// displacement of the previous client. Without it two attaches can
    /// acknowledge in one order and install in another, letting an older
    /// attach displace a newer one.
    attach: Mutex<()>,
    /// Monotonic identity of the latest client-slot ownership state.
    ///
    /// Advisory store updates carry the generation assigned under the client
    /// slot, so an older update that waited for store I/O cannot overwrite a
    /// newer attach or detach.
    attached_generation: Arc<AtomicU64>,
    /// Output the app produced before anyone attached, kept for the first
    /// client. Taken under the client slot, which is what orders it ahead of
    /// the output that follows the attach.
    preamble: Mutex<Option<Vec<u8>>>,
    /// The supervisor's first-attach lifetime gate.
    ///
    /// Holds an exited app's session open until a client has attached or the
    /// initiating startup connection has closed.
    first_attach: Mutex<FirstAttach>,
    first_attach_changed: Condvar,
    stopping: AtomicBool,
}

/// The connection that currently owns the console, and its write side.
struct Client<T: Transport> {
    conn: ConnId,
    outbox: Arc<Outbox<T>>,
}

impl<T: Transport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn,
            outbox: Arc::clone(&self.outbox),
        }
    }
}

/// State of the supervisor's first-attach lifetime gate.
///
/// An app that exits immediately would otherwise be torn down before `dure run`
/// finishes attaching, losing both its output and its exit status. The
/// supervisor therefore holds the session open after the app exits until one of
/// these two flags is set: a client has completed an attach, or the startup
/// connection the initiating `dure run` held has closed.
/// Ref: docs/design.md, "Commands"; docs/implementation.md, "Process split".
#[derive(Debug, Default)]
struct FirstAttach {
    /// Whether a client has ever taken the session.
    ///
    /// Never cleared: this records that the gate opened, not that a client is
    /// attached now. A client that later detaches leaves it set, because the
    /// session it was waiting for has already been claimed once.
    claimed: bool,
    /// Whether the process that started the session dropped its channel.
    initiator_gone: bool,
}

impl<T: Transport, C> Shared<T, C> {
    fn first_attach(&self) -> MutexGuard<'_, FirstAttach> {
        self.first_attach
            .lock()
            .expect("first-attach flags are only set, never held across a panic")
    }

    /// Records that a client took the session.
    // A mutation that drops either flag or the wait leaves the gate closed, and
    // watchdogs are disabled under cargo-mutants, so the test hangs instead of
    // failing.
    #[cfg_attr(test, mutants::skip)]
    fn note_claimed(&self) {
        self.first_attach().claimed = true;
        self.first_attach_changed.notify_all();
    }

    /// Records that the process that started the session dropped its channel.
    #[cfg_attr(test, mutants::skip)]
    fn note_initiator_gone(&self) {
        self.first_attach().initiator_gone = true;
        self.first_attach_changed.notify_all();
    }

    /// Blocks until the first client has attached or the initiating startup
    /// connection has closed.
    #[cfg_attr(test, mutants::skip)]
    fn await_first_attach(&self) {
        let mut state = self.first_attach();
        while !state.claimed && !state.initiator_gone {
            state = self
                .first_attach_changed
                .wait(state)
                .expect("first-attach flags are only set, never held across a panic");
        }
    }

    fn client(&self) -> MutexGuard<'_, Option<Client<T>>> {
        self.client
            .lock()
            .expect("client slot is only copied or replaced, never held across a panic")
    }

    /// Advances the identity of the client-slot ownership state.
    // A constant-return mutation makes later ownership updates indistinguishable.
    // The deterministic stall test then waits for an update that is correctly
    // discarded, and mutation watchdogs are disabled.
    #[cfg_attr(test, mutants::skip)]
    fn next_attached_generation(&self) -> u64 {
        let previous = self
            .attached_generation
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |generation| {
                generation.checked_add(1)
            })
            .expect("the process cannot perform enough ownership changes to exhaust u64");
        previous
            .checked_add(1)
            .expect("try_update only succeeds when the next generation exists")
    }

    /// Holds opening output: what the app wrote before the first attach.
    ///
    /// `dure run` starts the app and only then attaches, so an app that prints
    /// immediately writes into that window. Opening output is not scrollback —
    /// it is held for the first client rather than replayed to later ones.
    /// Ref: docs/design.md, "Screen contents".
    ///
    /// The caller holds the client slot, which is what keeps this from landing
    /// behind an attach that has already taken what was held.
    fn hold_for_first_client(&self, bytes: &[u8]) {
        let mut preamble = self
            .preamble
            .lock()
            .expect("the preamble is only appended to or taken, never held across a panic");
        let Some(held) = preamble.as_mut() else {
            return;
        };
        // The same measure of how far behind delivery has fallen that bounds a
        // live client's backlog. What is kept is the earliest output rather
        // than the latest, because a first screen is worth more to the arriving
        // client than the tail of a burst it has no context for.
        let free = MAX_CLIENT_BACKLOG_BYTES.saturating_sub(held.len());
        held.extend(bytes.iter().take(free));
    }

    /// Takes the opening output, permanently.
    ///
    /// Only the first attach receives it; a later attach finds nothing, which
    /// is what makes a resumed session start on an empty screen.
    /// Ref: docs/design.md, "Screen contents".
    fn take_preamble(&self) -> Option<Vec<u8>> {
        self.preamble
            .lock()
            .expect("the preamble is only appended to or taken, never held across a panic")
            .take()
            .filter(|held| !held.is_empty())
    }
}

/// Splits the opening output into frames the transport accepts.
///
/// The hold grows to `MAX_CLIENT_BACKLOG_BYTES`, which is several frames' worth,
/// and a receiver rejects any frame past the cap rather than reassembling it. A
/// single `Output` message would therefore fail the very attach it exists to
/// open, and would fail it precisely when the app had written the most.
/// Ref: docs/supervisor.md, "Opening output".
fn preamble_messages(held: &[u8]) -> impl Iterator<Item = Message> + use<'_> {
    held.chunks(MAX_OUTPUT_CHUNK_BYTES.get())
        .map(|chunk| Message::Output(chunk.to_vec()))
}

// Blocking serve loop. A mutation that returns before the accept and PTY threads
// are wired up leaves the test's client waiting forever, and watchdogs are
// disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn serve<P, S, T, C>(
    processes: &P,
    store: &S,
    transport: &T,
    pty_host: &C,
    initialized: &Initialized,
    startup: ConnId,
) -> Result<i32, AppError>
where
    P: Processes,
    S: SessionStore + Clone,
    T: Transport + Clone + Send + Sync + 'static,
    C: Pseudoconsole + Clone + Send + Sync + 'static,
{
    let Initialized {
        session_id,
        identity,
        listener,
        pty,
        job,
        app,
    } = *initialized;

    let record_live = Arc::new(Mutex::new(true));
    let attached_generation = Arc::new(AtomicU64::default());
    let shared = Arc::new(Shared {
        transport: transport.clone(),
        pty_host: pty_host.clone(),
        pty,
        session_id,
        client: Mutex::new(None),
        attach: Mutex::new(()),
        attached_generation: Arc::clone(&attached_generation),
        preamble: Mutex::new(Some(Vec::new())),
        first_attach: Mutex::new(FirstAttach::default()),
        first_attach_changed: Condvar::new(),
        stopping: AtomicBool::new(false),
    });

    // The startup channel doubles as the initiator's liveness signal: it stays
    // open for as long as `dure run` intends to attach.
    thread::spawn({
        let shared = Arc::clone(&shared);
        let transport = transport.clone();
        move || {
            _ = transport.recv(startup);
            transport.disconnect(startup);
            shared.note_initiator_gone();
        }
    });

    let store_flag = store_attached_flag(
        store,
        session_id,
        Arc::clone(&record_live),
        attached_generation,
    );
    thread::spawn({
        let shared = Arc::clone(&shared);
        let transport = transport.clone();
        move || accept_loop(&shared, &transport, listener, store_flag)
    });

    let pty_pump = thread::spawn({
        let shared = Arc::clone(&shared);
        move || pty_output_loop(&shared)
    });

    // Everything below this point is teardown the session owes the host whether
    // or not the wait succeeded: the listener, the job holding the app and its
    // descendants, the pseudoconsole, and the published record all outlive this
    // function otherwise. The wait failure is reported only once that is done.
    let waited = processes.wait_app(app);

    // An app can outlive neither its output nor its exit status: both are only
    // deliverable while the session is still up, so a session nobody has
    // attached to yet stays up until its initiator arrives or gives up. A wait
    // that failed has no status to deliver, so there is nothing to wait for.
    if waited.is_ok() {
        shared.await_first_attach();
    }

    transport.close_listener(listener);
    // Descendants of the app stay attached to the pseudoconsole until this job
    // ends them, and closing a pseudoconsole waits for its attached clients.
    processes.close_job(job);
    // The app has exited, so ending the pseudoconsole flushes what it still
    // holds and lets the output loop finish those bytes before the read fails.
    // Joining the pump before announcing the exit is what orders the app's final
    // output ahead of `AppExited` instead of racing it.
    pty_host.finish(pty);
    _ = pty_pump.join();
    pty_host.close(pty);

    // Both under the attach lock, which `client_loop` also takes for the whole
    // attach transaction. An attach therefore either completes before the slot
    // is claimed here and receives the exit status, or observes the stop and is
    // refused. Reading the slot outside the lock would let a client install
    // itself between the two and never learn that the app exited.
    let client = {
        let _attach = shared
            .attach
            .lock()
            .expect("the attach lock guards no data, so it is never poisoned by its guard");
        shared.stopping.store(true, Ordering::SeqCst);
        shared.client().take()
    };
    if let Some(client) = &client {
        if let Ok(status) = &waited {
            // Attach treats a disconnect without `AppExited` as a relay failure
            // when the input thread has already stopped, so the status must be
            // queued behind the output rather than racing it.
            client.outbox.send(Message::AppExited { status: *status });
        }
        client.outbox.finish();
    }

    let deleted = {
        // Client threads take this lock before publishing an attached-flag
        // update. Clearing it first prevents a late publish from recreating
        // the record after delete, including over a reused session id.
        let mut live = record_live
            .lock()
            .expect("record_live is only set false here, never held across a panic");
        *live = false;
        // Ids are reused, so an unconditional delete could reap whichever
        // session claimed this id after this supervisor published.
        store.delete_owned_by(session_id, &identity)
    };

    // A wait that failed is the cause and a record that outlives it is only a
    // consequence, so the wait failure is the one worth reporting.
    let status = waited.map_err(PalFailedError::caused_by)?;
    deleted.map_err(StoreError::caused_by)?;

    if let Some(client) = client {
        // The session already owns nothing, so waiting here for the exit status
        // to land costs a client that is still reading nothing and a client
        // that has stopped reading only this process outliving it.
        client.outbox.wait_for_writer();
    }
    Ok(status)
}

// A mutation that skips a live record's update leaves the deterministic
// store-stall test waiting for an operation that can never arrive, and
// watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn store_attached_flag<S: SessionStore + Clone>(
    store: &S,
    id: SessionId,
    record_live: Arc<Mutex<bool>>,
    current_generation: Arc<AtomicU64>,
) -> impl Fn(u64, bool) + Clone + Send + 'static {
    let store = store.clone();
    move |generation: u64, attached: bool| {
        let live = record_live
            .lock()
            .expect("record_live is only set false at delete, never held across a panic");
        if !*live || current_generation.load(Ordering::SeqCst) != generation {
            return;
        }
        if let Ok(Some(mut record)) = store.read(id) {
            record.attached = attached;
            // A failed write leaves a stale attached flag. The flag is
            // advisory; liveness is the supervisor process.
            _ = store.publish(&record);
        }
    }
}

// Blocking accept. A mutation that drops the stop check or the accept error
// path hangs unit tests because watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn accept_loop<T, C>(
    shared: &Arc<Shared<T, C>>,
    transport: &T,
    listener: ListenerId,
    set_attached: impl Fn(u64, bool) + Clone + Send + 'static,
) where
    T: Transport + Clone,
    C: Pseudoconsole,
{
    while !shared.stopping.load(Ordering::SeqCst) {
        let Ok(conn) = transport.accept(listener) else {
            break;
        };
        // Steal happens after a valid Attach in `client_loop`. Installing the
        // slot on accept would let a stalled connection displace a live client
        // and inject Output before Attached.
        thread::spawn({
            let shared = Arc::clone(shared);
            let set_attached = set_attached.clone();
            move || client_loop(&shared, conn, &set_attached)
        });
    }
}

// Blocking recv. A mutation that drops the disconnect path hangs unit tests
// because watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn client_loop<T, C>(shared: &Shared<T, C>, conn: ConnId, set_attached: &impl Fn(u64, bool))
where
    T: Transport + Clone,
    C: Pseudoconsole,
{
    match shared.transport.recv_timeout(conn, CONNECT_TIMEOUT) {
        Ok(Message::Attach { cols, rows }) => {
            // One serialized attach transaction: acknowledge, take ownership,
            // and displace the previous client without another attach
            // interleaving between the acknowledgement and the transfer.
            let _attach = shared
                .attach
                .lock()
                .expect("the attach lock guards no data, so it is never poisoned by its guard");
            // The exit status is routed to whoever owns the slot at the moment
            // teardown claims it, under this same lock. An attach that installed
            // itself afterwards would own a session that has already given up
            // its record, job, and pseudoconsole, and would lose the supervisor
            // without ever being told the app exited. Refusing before
            // acknowledging is what makes `resume` report a session that is gone
            // instead of a relay that broke.
            if shared.stopping.load(Ordering::SeqCst) {
                shared.transport.disconnect(conn);
                return;
            }
            let outbox = Outbox::start(shared.transport.clone(), conn);
            let (previous, generation) = {
                let mut slot = shared.client();
                // Queued, never written here. The acknowledgement is the first
                // thing on this connection's queue, and delivery is FIFO, so
                // the client sees `Attached` before any output or exit status
                // without this critical section waiting on a pipe write. A
                // client that is already gone is detected by that write failing
                // on the writer thread, which abandons the connection and ends
                // this loop.
                outbox.send(Message::Attached {
                    session_id: shared.session_id,
                });
                if let Some(held) = shared.take_preamble() {
                    for message in preamble_messages(&held) {
                        outbox.send(message);
                    }
                }
                let previous = slot.replace(Client {
                    conn,
                    outbox: Arc::clone(&outbox),
                });
                let generation = shared.next_attached_generation();
                (previous, generation)
            };
            // The client owns the slot now. Signaling the supervisor's
            // first-attach lifetime gate lets it finish delivering an
            // already-exited app's output and status before the advisory store
            // update below can encounter durable I/O.
            shared.note_claimed();
            if let Some(old) = previous {
                // Queued rather than written here, so a client that stopped
                // reading cannot hold up the steal that is replacing it. The
                // displaced client may already have disconnected; steal still
                // proceeds, because last-attach-wins does not depend on this
                // notice.
                //
                // A client that is alive but has stopped draining leaves its
                // writer blocked here until the client's own process exits and
                // closes the pipe. That is accepted: the notice is what tells a
                // user why their screen went quiet, and it is worth more than
                // reclaiming the thread promptly.
                // Ref: docs/supervisor.md, "Displacement".
                old.outbox.send(Message::Displaced);
                old.outbox.finish();
            }
            // Applied only once this connection owns the client slot: the app
            // redraws in response to a size change, and that redraw belongs to
            // the client that asked for the size. Resize failure means the pty
            // is already gone; wait_app and read_output observe that and stop
            // the relay.
            // Ref: docs/console.md, "Window size".
            _ = shared
                .pty_host
                .resize(shared.pty, WindowSize { cols, rows });
            // Store I/O is not part of the serialized ownership transfer. A
            // stalled durable write must not prevent teardown or another client
            // from acquiring the attach lock.
            drop(_attach);
            set_attached(generation, true);
        }
        _ => {
            // Anything else — a client that connected and then said nothing
            // within the connect budget, or one that opened with a message
            // that is not an attach — never becomes the live console, so it is
            // dropped rather than left holding a relay thread.
            shared.transport.disconnect(conn);
            return;
        }
    }

    while let Ok(message) = shared.transport.recv(conn) {
        // Ownership is checked and the message applied under one lock: a client
        // displaced while its receive was in flight must not reach the app
        // after the new client became the live console. Holding the lock across
        // the write is bounded because the console host drains its input pipe
        // whether or not the app reads it.
        // Ref: docs/supervisor.md, "Displacement".
        let slot = shared.client();
        if slot.as_ref().map(|client| client.conn) != Some(conn) {
            break;
        }
        match message {
            Message::Input(data) => {
                // Input after the app has exited is dropped. wait_app publishes
                // AppExited to the live client.
                _ = shared.pty_host.write_input(shared.pty, &data);
            }
            Message::Resize { cols, rows } => {
                _ = shared
                    .pty_host
                    .resize(shared.pty, WindowSize { cols, rows });
            }
            _ => break,
        }
        drop(slot);
    }
    let (departing, generation) = {
        let mut slot = shared.client();
        if slot.as_ref().map(|client| client.conn) == Some(conn) {
            let departing = slot.take();
            let generation = shared.next_attached_generation();
            (departing, Some(generation))
        } else {
            (None, None)
        }
    };
    if let Some(generation) = generation {
        // Store I/O is serialized by generation rather than by the client slot.
        // A stalled advisory write therefore cannot block ownership transfer.
        set_attached(generation, false);
    }
    if let Some(departing) = departing {
        // The peer is gone or misbehaving, so nothing still queued for it is
        // worth waiting on.
        departing.outbox.abandon();
    }
}

// Blocking read of pty output. A mutation that drops the stop check hangs
// unit tests because watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn pty_output_loop<T, C>(shared: &Shared<T, C>)
where
    T: Transport + Clone,
    C: Pseudoconsole,
{
    while !shared.stopping.load(Ordering::SeqCst) {
        // A failed read leaves the app's output incomplete, but there is
        // nothing further to relay either way, so both end the pump; the
        // difference is that a clean end means everything the app wrote has
        // been delivered.
        let Ok(Some(bytes)) = shared.pty_host.read_output(shared.pty) else {
            break;
        };
        // Queued, never written here: a client that stopped reading must not be
        // able to hold the pump, which the exit teardown joins. A write failure
        // is handled by the outbox dropping the connection, which ends the
        // client's own loop and clears the slot.
        let client = {
            let slot = shared.client();
            match slot.as_ref() {
                Some(client) => Some(client.clone()),
                None => {
                    shared.hold_for_first_client(&bytes);
                    None
                }
            }
        };
        if let Some(client) = client {
            client.outbox.send(Message::Output(bytes));
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::thread;

    use testing::{WatchdogPhaseReporter, with_watchdog_phases};

    use super::*;
    use crate::durability::LauncherTie;
    use crate::pal::ids::{AppId, ConnId, JobId, ListenerId};
    use crate::pal::processes::MockProcesses;
    use crate::pal::pseudoconsole::MemoryPseudoconsole;
    use crate::pal::session_store::MemorySessionStore;
    use crate::pal::transport::MemoryTransport;
    use crate::protocol::{Message, encode, payload_len_ok};
    use crate::session_record::ProcessIdentity;

    /// Arbitrary nonzero status the mock app exits with, so a test can tell a
    /// forwarded status from a defaulted one.
    const SAMPLE_APP_EXIT: i32 = 7;

    /// Ordinary valid geometry for tests where resize behavior is out of scope.
    const ORDINARY_ATTACH: Message = Message::Attach { cols: 80, rows: 24 };

    /// Completes the client side of the startup commit handshake.
    fn commit_startup(
        transport: &MemoryTransport,
        listener: ListenerId,
        phase_reporter: &WatchdogPhaseReporter,
    ) -> (ConnId, SessionId, LauncherTie) {
        phase_reporter.report("waiting for the supervisor startup connection");
        let conn = transport.accept(listener).unwrap();
        phase_reporter.report("waiting for the supervisor startup response");
        let Message::StartupOk {
            session_id,
            launcher_tie,
        } = transport.recv(conn).unwrap()
        else {
            panic!("expected startup ok");
        };
        transport.send(conn, &Message::StartupCommit).unwrap();
        (conn, session_id, launcher_tie)
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

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn steal_displaces_first_client() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(false), Condvar::new()));
            let processes = mock_processes(Arc::clone(&exit));

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);
            transport.disconnect(startup_conn);

            let pipe = transport.pipe_name("nonce");
            let first = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
            transport.send(first, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the first attach acknowledgement");
            assert!(matches!(
                transport.recv(first).unwrap(),
                Message::Attached { session_id: id } if id == session_id
            ));

            let second = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
            transport.send(second, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the second attach acknowledgement");
            assert!(matches!(
                transport.recv(second).unwrap(),
                Message::Attached { .. }
            ));
            phase_reporter.report("waiting for the displacement notice");
            assert!(matches!(transport.recv(first).unwrap(), Message::Displaced));

            {
                let (lock, cvar) = &*exit;
                *lock.lock().expect("exit lock") = true;
                cvar.notify_all();
            }
            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
            assert!(store.list().unwrap().is_empty());
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn final_output_arrives_before_the_exit_status() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(false), Condvar::new()));
            let processes = mock_processes(Arc::clone(&exit));

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let pty = pty.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);
            transport.disconnect(startup_conn);

            let client = transport
                .connect(&transport.pipe_name("nonce"), CONNECT_TIMEOUT)
                .unwrap();
            transport.send(client, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the attach acknowledgement");
            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));

            // Withholding puts the bytes past the pump's reach, modelling output
            // still in flight when the app exits. Only an orderly shutdown can
            // deliver them, so a teardown that abandons the console instead
            // fails here rather than intermittently.
            let app_console = pty.only_pty();
            pty.withhold_output(app_console);
            pty.push_output(app_console, b"bye");
            {
                let (lock, cvar) = &*exit;
                *lock.lock().expect("exit lock") = true;
                cvar.notify_all();
            }

            phase_reporter.report("waiting for final app output");
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::Output(b"bye".to_vec())
            );
            phase_reporter.report("waiting for the app exit status");
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::AppExited {
                    status: SAMPLE_APP_EXIT
                }
            );
            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn an_app_that_exits_before_anyone_attaches_still_reports_its_status() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            // The app is already gone by the time the supervisor waits on it.
            let exit = Arc::new((Mutex::new(true), Condvar::new()));
            let processes = mock_processes(Arc::clone(&exit));

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);

            // The startup connection stays open, which is what holds the
            // session up for the attach that `dure run` is about to make.
            let client = transport
                .connect(&transport.pipe_name("nonce"), CONNECT_TIMEOUT)
                .unwrap();
            transport.send(client, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the attach acknowledgement");
            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));
            phase_reporter.report("waiting for the app exit status");
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::AppExited {
                    status: SAMPLE_APP_EXIT
                }
            );
            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
            transport.disconnect(startup_conn);
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_stalled_attached_flag_does_not_delay_the_exit_status() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            // The app is already gone, so attaching must immediately release
            // the supervisor to report its status.
            let exit = Arc::new((Mutex::new(true), Condvar::new()));
            let processes =
                mock_processes_with_close_job(exit, LauncherTie::NoneDetected, AppWait::Reports, {
                    let store = store.clone();
                    // Hold teardown after the first-attach lifetime gate opens
                    // but before it claims the client slot and invalidates the
                    // record, so the advisory write reaches its injected stall.
                    move |_| store.wait_for_stalled_publish()
                });

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);
            store.stall_publishes();

            let client = transport
                .connect(&transport.pipe_name("nonce"), CONNECT_TIMEOUT)
                .unwrap();
            transport.send(client, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the attach acknowledgement");
            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));

            phase_reporter.report("waiting for the attached-flag publication to stall");
            store.wait_for_stalled_publish();
            // Reaching the exit status proves both that the first-attach signal
            // preceded the advisory write and that teardown acquired the attach
            // lock while the write remained stalled.
            phase_reporter.report("waiting for the app exit status");
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::AppExited {
                    status: SAMPLE_APP_EXIT
                }
            );

            store.resume_publishes();
            transport.disconnect(client);
            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
            transport.disconnect(startup_conn);
            assert!(store.list().unwrap().is_empty());
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_session_nobody_comes_for_ends_when_its_initiator_gives_up() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(true), Condvar::new()));
            let processes = mock_processes(Arc::clone(&exit));

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);
            // Nobody will ever attach, so the gate must open on this instead.
            transport.disconnect(startup_conn);
            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
            assert!(store.list().unwrap().is_empty());
        });
    }

    /// Client behavior that rejects a provisional startup transaction.
    enum RejectedStartup {
        Disconnect,
        Message(Message),
        Timeout,
    }

    fn assert_rejected_startup_rolls_back(rejection: RejectedStartup) {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(true), Condvar::new()));
            let processes = mock_processes(exit);

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            phase_reporter.report("waiting for the supervisor startup connection");
            let startup_conn = transport.accept(startup).unwrap();
            phase_reporter.report("waiting for the supervisor startup response");
            assert!(matches!(
                transport.recv(startup_conn).unwrap(),
                Message::StartupOk { .. }
            ));
            let records = store.list().unwrap();
            let [record] = records.as_slice() else {
                panic!("expected exactly one record");
            };
            let session_pipe = record.pipe_name.clone();
            match rejection {
                RejectedStartup::Disconnect => transport.disconnect(startup_conn),
                RejectedStartup::Message(acknowledgement) => {
                    transport.send(startup_conn, &acknowledgement).unwrap();
                }
                RejectedStartup::Timeout => transport.expire_next_recv("startup"),
            }

            phase_reporter.report("waiting for startup rollback");
            let error = supervisor.join().unwrap().unwrap_err();
            assert!(error.find_source::<StartupFailedError>().is_some());
            assert!(store.list().unwrap().is_empty());
            transport
                .connect(&session_pipe, CONNECT_TIMEOUT)
                .unwrap_err();
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_session_without_startup_commit_is_rolled_back() {
        assert_rejected_startup_rolls_back(RejectedStartup::Disconnect);
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_session_with_an_invalid_startup_commit_is_rolled_back() {
        assert_rejected_startup_rolls_back(RejectedStartup::Message(Message::StartupErr));
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_startup_commit_timeout_is_rolled_back() {
        assert_rejected_startup_rolls_back(RejectedStartup::Timeout);
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn failure_to_send_startup_ok_rolls_back() {
        with_watchdog_phases("running the rejected startup", |_phase_reporter| {
            let transport = MemoryTransport::new();
            transport.fail_next_send("startup");
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(true), Condvar::new()));
            let processes = mock_processes(exit);

            let _startup = transport.listen("startup").unwrap();
            let error = run_supervisor(
                &processes,
                &store,
                &transport,
                &pty,
                "startup",
                PathBuf::from("/work"),
                AppCommand::for_test(&["app.exe"]),
            )
            .unwrap_err();

            assert!(error.find_source::<StartupFailedError>().is_some());
            assert!(store.list().unwrap().is_empty());
        });
    }

    #[test]
    fn init_failure_sends_startup_err_and_closes_job() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let mut processes = MockProcesses::new();
            processes
                .expect_launcher_tie()
                .returning(|| LauncherTie::NoneDetected);
            processes
                .expect_create_lifetime_job()
                .returning(|| Ok(JobId::for_test(1)));
            // Spawn fails before initialization constructs the session record,
            // so this path never reads the system clock.
            processes
                .expect_spawn_app()
                .returning(|_| Err(PalError::new(PalErrorKind::Other)));
            processes.expect_close_job().times(1).returning(|_| ());

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            phase_reporter.report("waiting for the supervisor startup connection");
            let startup_conn = transport.accept(startup).unwrap();
            phase_reporter.report("waiting for the startup error");
            assert_eq!(transport.recv(startup_conn).unwrap(), Message::StartupErr);
            phase_reporter.report("waiting for startup rollback");
            supervisor.join().unwrap().unwrap_err();
            assert!(store.list().unwrap().is_empty());
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_cannot_outlive_its_launcher_says_so_on_the_startup_pipe() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let exit = Arc::new((Mutex::new(false), Condvar::new()));
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let processes =
                mock_processes_with(Arc::clone(&exit), LauncherTie::Confirmed, AppWait::Reports);

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            // Only the client has a console to report this on.
            let (startup_conn, _session_id, launcher_tie) =
                commit_startup(&transport, startup, &phase_reporter);
            assert_eq!(launcher_tie, LauncherTie::Confirmed);
            transport.disconnect(startup_conn);
            {
                let (lock, cvar) = &*exit;
                *lock.lock().expect("exit lock") = true;
                cvar.notify_all();
            }
            phase_reporter.report("waiting for supervisor shutdown");
            supervisor.join().unwrap().unwrap();
        });
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_wait_that_fails_still_takes_the_session_off_the_host() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let exit = Arc::new((Mutex::new(false), Condvar::new()));
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let processes =
                mock_processes_with(Arc::clone(&exit), LauncherTie::NoneDetected, AppWait::Fails);

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let store = store.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);
            assert_eq!(store.list().unwrap().len(), 1, "the session was published");
            transport.disconnect(startup_conn);
            {
                let (lock, cvar) = &*exit;
                *lock.lock().expect("exit lock") = true;
                cvar.notify_all();
            }

            phase_reporter.report("waiting for failed-wait cleanup");
            supervisor.join().unwrap().unwrap_err();
            // The wait is the only thing that failed, so everything the session
            // put on the host is still the session's to take back.
            assert!(
                store.list().unwrap().is_empty(),
                "the record outlived the session"
            );
        });
    }

    #[test]
    fn breakaway_denial_keeps_its_identity() {
        let denied = map_startup(&PalError::new(PalErrorKind::BreakawayDenied));
        assert!(denied.find_source::<BreakawayDeniedError>().is_some());
        let other = map_startup(&PalError::new(PalErrorKind::Other));
        assert!(other.find_source::<StartupFailedError>().is_some());
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn output_produced_before_the_first_attach_reaches_that_client() {
        with_watchdog_phases("setting up the supervisor", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty = MemoryPseudoconsole::new();
            let store = MemorySessionStore::new();
            let exit = Arc::new((Mutex::new(false), Condvar::new()));
            let processes = mock_processes(Arc::clone(&exit));

            let startup = transport.listen("startup").unwrap();
            let supervisor = thread::spawn({
                let transport = transport.clone();
                let pty = pty.clone();
                move || {
                    run_supervisor(
                        &processes,
                        &store,
                        &transport,
                        &pty,
                        "startup",
                        PathBuf::from("/work"),
                        AppCommand::for_test(&["app.exe"]),
                    )
                }
            });

            let (startup_conn, _session_id, _durability) =
                commit_startup(&transport, startup, &phase_reporter);

            // The app speaks before anyone has attached, which is the window
            // `dure run` spends spawning the supervisor and connecting to it.
            pty.push_output(pty.only_pty(), b"hello");
            transport.disconnect(startup_conn);

            let client = transport
                .connect(&transport.pipe_name("nonce"), CONNECT_TIMEOUT)
                .unwrap();
            transport.send(client, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the attach acknowledgement");
            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));

            {
                let (lock, cvar) = &*exit;
                *lock.lock().expect("exit lock") = true;
                cvar.notify_all();
            }

            // Reading until the app's exit status rather than expecting the held
            // bytes as the very next message keeps this test terminating: a
            // supervisor that never delivers them still reaches `AppExited`, so
            // the failure is an assertion rather than a wait with no end.
            let mut received = Vec::new();
            loop {
                phase_reporter.report("waiting for held output or the app exit status");
                match transport.recv(client).unwrap() {
                    Message::Output(bytes) => received.extend(bytes),
                    Message::AppExited { status } => {
                        assert_eq!(status, SAMPLE_APP_EXIT);
                        break;
                    }
                    other => panic!("unexpected message {other:?}"),
                }
            }
            assert_eq!(received, b"hello");

            phase_reporter.report("waiting for supervisor shutdown");
            assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        });
    }

    #[test]
    fn only_the_first_client_receives_what_was_held_for_it() {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty);
        shared.hold_for_first_client(b"hello");
        assert_eq!(shared.take_preamble(), Some(b"hello".to_vec()));
        // A client that attaches later starts on an empty screen, so output
        // produced while nobody was attached is not kept for it.
        shared.hold_for_first_client(b"unheard");
        assert_eq!(shared.take_preamble(), None);
    }

    #[test]
    fn nothing_held_is_nothing_to_deliver() {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty);
        assert_eq!(shared.take_preamble(), None);
    }

    #[test]
    // Moves megabytes through pure byte handling, so Miri has no unsafe code to
    // check here and its interpreter makes the volume impractically slow.
    #[cfg_attr(miri, ignore)]
    fn what_is_held_for_the_first_client_is_capped() {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty);
        let chunk = vec![b'x'; 64 * 1024];
        let rounds = MAX_CLIENT_BACKLOG_BYTES.div_euclid(chunk.len()) + 2;
        for _ in 0..rounds {
            shared.hold_for_first_client(&chunk);
        }
        assert_eq!(
            shared.take_preamble().map(|held| held.len()),
            Some(MAX_CLIENT_BACKLOG_BYTES)
        );
    }

    #[test]
    // Moves megabytes through pure byte handling, so Miri has no unsafe code to
    // check here and its interpreter makes the volume impractically slow.
    #[cfg_attr(miri, ignore)]
    fn a_preamble_too_large_for_one_frame_is_split_into_frames_a_receiver_accepts() {
        // A hold the transport could not carry in one frame, which is reachable
        // because the hold cap is several frames' worth.
        let held = vec![b'x'; MAX_OUTPUT_CHUNK_BYTES.get().saturating_add(1)];
        const {
            assert!(
                MAX_CLIENT_BACKLOG_BYTES > MAX_OUTPUT_CHUNK_BYTES.get(),
                "a hold that cannot outgrow one frame would make this test vacuous"
            );
        }

        let messages: Vec<Message> = preamble_messages(&held).collect();

        assert!(messages.len() > 1, "the hold was not split");
        let mut rejoined = Vec::new();
        for message in &messages {
            let frame = encode(message);
            let prefix: [u8; 4] = frame
                .get(..4)
                .expect("a frame carries a length prefix")
                .try_into()
                .unwrap();
            assert!(
                payload_len_ok(u32::from_le_bytes(prefix)),
                "a receiver would reject this frame"
            );
            match message {
                Message::Output(bytes) => rejoined.extend_from_slice(bytes),
                other => panic!("the hold must be relayed as output, got {other:?}"),
            }
        }
        // Splitting must not lose or reorder what the app said.
        assert_eq!(rejoined, held);
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

    #[test]
    fn a_client_that_leaves_before_it_is_acknowledged_leaves_the_slot_empty() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");

        transport.send(client, &ORDINARY_ATTACH).unwrap();
        transport.disconnect(client);

        let (flags, recorder) = attach_recorder();
        client_loop(&shared, supervisor, &recorder);

        // The session is never left believing a client that has gone still owns
        // it, however far into the attach that client got.
        assert!(client_conn(&shared).is_none());
        assert_eq!(
            flags.lock().unwrap().last().copied(),
            Some(false),
            "the last thing published about a departed client must be that it left"
        );
    }

    #[test]
    fn a_stalled_attach_acknowledgement_does_not_hold_up_the_attach() {
        with_watchdog_phases("setting up the client relay", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty_host = MemoryPseudoconsole::new();
            let shared = Arc::new(shared_session(&transport, &pty_host));
            let (supervisor, client) = connected_pair(&transport, "pipe");

            let (attached_tx, attached_rx) = mpsc::channel();
            transport.send(client, &ORDINARY_ATTACH).unwrap();
            transport.stall(supervisor);
            let relay = thread::spawn({
                let shared = Arc::clone(&shared);
                move || {
                    client_loop(&shared, supervisor, &|_generation, attached| {
                        attached_tx.send(attached).unwrap();
                    });
                }
            });

            phase_reporter.report("waiting for the attach acknowledgement to stall");
            transport.wait_for_stalled_send(supervisor);
            // The acknowledgement is queued, so a client that has stopped
            // reading holds up only its own delivery: ownership has already
            // transferred and the session is claimed.
            phase_reporter.report("waiting for the attached-flag update");
            assert!(attached_rx.recv().unwrap());
            assert!(shared.first_attach().claimed);

            transport.resume(supervisor);
            phase_reporter.report("waiting for the attach acknowledgement");
            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));

            transport.send(client, &Message::StartupErr).unwrap();
            phase_reporter.report("waiting for the client relay to stop");
            relay.join().unwrap();
            assert!(!attached_rx.recv().unwrap());
        });
    }

    #[test]
    fn a_stalled_detach_update_does_not_block_or_overwrite_a_steal() {
        with_watchdog_phases("setting up the client relays", |phase_reporter| {
            let transport = MemoryTransport::new();
            let pty_host = MemoryPseudoconsole::new();
            let shared = Arc::new(shared_session(&transport, &pty_host));
            let store = MemorySessionStore::new();
            let owner = ProcessIdentity::for_test(1);
            let id = store.allocate_id(&owner).unwrap();
            assert_eq!(id, shared.session_id);
            store
                .publish(&SessionRecord {
                    id: id.get(),
                    supervisor_pid: owner.pid,
                    supervisor_creation_time: owner.creation_time,
                    pipe_name: "pipe".to_string(),
                    launch_directory: PathBuf::from("/work"),
                    command: AppCommand::for_test(&["app.exe"]),
                    started_at_unix_ms: 1,
                    attached: false,
                })
                .unwrap();
            let record_live = Arc::new(Mutex::new(true));
            let set_attached = store_attached_flag(
                &store,
                id,
                record_live,
                Arc::clone(&shared.attached_generation),
            );
            let (updated_tx, updated_rx) = mpsc::channel();
            let observe_update = move |generation, attached| {
                set_attached(generation, attached);
                updated_tx.send(attached).unwrap();
            };

            let (first_supervisor, first_client) = connected_pair(&transport, "first");
            let first_relay = thread::spawn({
                let shared = Arc::clone(&shared);
                let observe_update = observe_update.clone();
                move || client_loop(&shared, first_supervisor, &observe_update)
            });
            transport.send(first_client, &ORDINARY_ATTACH).unwrap();
            phase_reporter.report("waiting for the first attach acknowledgement");
            assert!(matches!(
                transport.recv(first_client).unwrap(),
                Message::Attached { .. }
            ));
            phase_reporter.report("waiting for the first attached-flag update");
            assert!(updated_rx.recv().unwrap());

            store.stall_publishes();
            transport.disconnect(first_client);
            phase_reporter.report("waiting for the detached-flag update to stall");
            store.wait_for_stalled_publish();

            let (second_supervisor, second_client) = connected_pair(&transport, "second");
            let second_relay = thread::spawn({
                let shared = Arc::clone(&shared);
                move || client_loop(&shared, second_supervisor, &observe_update)
            });
            transport.send(second_client, &ORDINARY_ATTACH).unwrap();
            // The first relay is blocked in store I/O. Receiving this proves it
            // released the client slot before publishing the advisory flag.
            phase_reporter.report("waiting for the stealing attach acknowledgement");
            assert!(matches!(
                transport.recv(second_client).unwrap(),
                Message::Attached { .. }
            ));

            store.resume_publishes();
            phase_reporter.report("waiting for the stalled detach and newer attach updates");
            let completed = [updated_rx.recv().unwrap(), updated_rx.recv().unwrap()];
            assert!(completed.contains(&false));
            assert!(completed.contains(&true));
            first_relay.join().unwrap();
            assert!(store.read(id).unwrap().unwrap().attached);

            transport.disconnect(second_client);
            phase_reporter.report("waiting for the final detach update");
            assert!(!updated_rx.recv().unwrap());
            second_relay.join().unwrap();
            assert!(!store.read(id).unwrap().unwrap().attached);
        });
    }

    #[test]
    fn an_attach_that_arrives_after_teardown_claims_the_slot_is_refused() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");
        // Teardown has already routed the exit status to whoever owned the
        // slot, so there is nothing left for a new client to be given.
        shared.stopping.store(true, Ordering::SeqCst);

        transport.send(client, &ORDINARY_ATTACH).unwrap();

        let (flags, recorder) = attach_recorder();
        client_loop(&shared, supervisor, &recorder);

        // Refused before the acknowledgement, so the client sees a session that
        // is gone rather than one that broke mid-relay.
        assert!(client_conn(&shared).is_none());
        assert!(flags.lock().unwrap().is_empty());
        transport.recv(client).unwrap_err();
    }

    #[test]
    fn a_client_that_does_not_attach_first_is_dropped() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");

        transport
            .send(client, &Message::Input(b"x".to_vec()))
            .unwrap();

        let (flags, recorder) = attach_recorder();
        client_loop(&shared, supervisor, &recorder);

        assert!(client_conn(&shared).is_none());
        assert!(flags.lock().unwrap().is_empty());
        assert!(pty_host.take_input(shared.pty).is_empty());
        transport.recv(client).unwrap_err();
    }

    #[test]
    fn the_relay_forwards_input_and_resize_until_the_client_stops() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");

        transport.send(client, &ORDINARY_ATTACH).unwrap();
        transport
            .send(
                client,
                &Message::Resize {
                    cols: 120,
                    rows: 40,
                },
            )
            .unwrap();
        transport
            .send(client, &Message::Input(b"hi".to_vec()))
            .unwrap();
        // Anything the supervisor does not relay ends the client loop.
        transport.send(client, &Message::StartupErr).unwrap();

        let (flags, recorder) = attach_recorder();
        client_loop(&shared, supervisor, &recorder);

        assert_eq!(
            pty_host.size(shared.pty),
            Some(WindowSize {
                cols: 120,
                rows: 40
            })
        );
        assert_eq!(pty_host.take_input(shared.pty), b"hi");
        assert!(client_conn(&shared).is_none());
        assert_eq!(*flags.lock().unwrap(), vec![true, false]);
    }

    #[test]
    fn a_displaced_relay_leaves_the_new_client_installed() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = Arc::new(shared_session(&transport, &pty_host));
        let (supervisor, client) = connected_pair(&transport, "pipe");
        let (successor, _successor_client) = connected_pair(&transport, "successor");
        let successor_outbox = Outbox::start(transport.clone(), successor);
        let displaced = Arc::new(Mutex::new(None));
        // Stands in for a steal that lands between the acknowledgement and the
        // first relayed message. The displaced relay must not reach the app.
        let steal = {
            let shared = Arc::clone(&shared);
            let successor_outbox = Arc::clone(&successor_outbox);
            let displaced = Arc::clone(&displaced);
            move |_generation: u64, attached: bool| {
                if attached {
                    let previous = shared.client().replace(Client {
                        conn: successor,
                        outbox: Arc::clone(&successor_outbox),
                    });
                    // What the real steal does to the client it replaces.
                    if let Some(previous) = previous {
                        previous.outbox.finish();
                        *displaced.lock().unwrap() = Some(previous.outbox);
                    }
                }
            }
        };

        transport.send(client, &ORDINARY_ATTACH).unwrap();
        transport
            .send(client, &Message::Input(b"x".to_vec()))
            .unwrap();

        client_loop(&shared, supervisor, &steal);

        assert!(pty_host.take_input(shared.pty).is_empty());
        assert_eq!(client_conn(&shared), Some(successor));

        displaced
            .lock()
            .unwrap()
            .take()
            .expect("the steal displaced the first client")
            .wait_for_writer();
        successor_outbox.finish();
        successor_outbox.wait_for_writer();
    }

    #[test]
    fn output_is_relayed_to_the_installed_client() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");
        let outbox = Outbox::start(transport.clone(), supervisor);
        *shared.client() = Some(Client {
            conn: supervisor,
            outbox: Arc::clone(&outbox),
        });

        pty_host.push_output(shared.pty, b"out");
        // Ends the loop once the output has been drained.
        pty_host.finish(shared.pty);

        pty_output_loop(&shared);
        outbox.finish();
        outbox.wait_for_writer();

        assert!(matches!(
            transport.recv(client).unwrap(),
            Message::Output(bytes) if bytes == b"out",
        ));
    }

    #[test]
    fn output_for_a_client_that_is_gone_is_discarded() {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = shared_session(&transport, &pty_host);
        let (supervisor, client) = connected_pair(&transport, "pipe");
        let outbox = Outbox::start(transport.clone(), supervisor);
        *shared.client() = Some(Client {
            conn: supervisor,
            outbox: Arc::clone(&outbox),
        });
        transport.disconnect(client);

        pty_host.push_output(shared.pty, b"out");
        // Ends the loop once the undeliverable output has been drained.
        pty_host.finish(shared.pty);

        // The pump must not be the thread that notices, so it completes even
        // though nothing can be delivered.
        pty_output_loop(&shared);
        outbox.wait_for_writer();
    }

    #[test]
    // Building the session record reads the real system clock.
    #[cfg_attr(miri, ignore)]
    fn a_failure_after_id_allocation_releases_the_id() {
        let store = MemorySessionStore::new();
        store.fail_next_publish();
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes = mock_processes(exit);

        {
            let mut guard = InitGuard {
                processes: &processes,
                store: &store,
                transport: &transport,
                pty_host: &pty,
                job: None,
                pty: None,
                listener: None,
                session: None,
                committed: false,
            };
            let error = initialize(
                &mut guard,
                &processes,
                &store,
                &transport,
                &pty,
                PathBuf::from("/work"),
                AppCommand::for_test(&["app.exe"]),
            )
            .err()
            .expect("record publication fails");
            assert!(error.find_source::<StoreError>().is_some());
        }

        assert!(store.list_reservations().unwrap().is_empty());
    }

    #[test]
    fn attached_flag_publishes_only_the_current_generation_while_the_record_lives() {
        let store = MemorySessionStore::new();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store
            .publish(&SessionRecord {
                id: id.get(),
                supervisor_pid: 10,
                supervisor_creation_time: 100,
                pipe_name: "pipe".to_string(),
                launch_directory: PathBuf::from("/work"),
                command: AppCommand::for_test(&["app.exe"]),
                started_at_unix_ms: 1,
                attached: false,
            })
            .unwrap();

        let record_live = Arc::new(Mutex::new(true));
        let current_generation = Arc::new(AtomicU64::new(2));
        let set_attached = store_attached_flag(
            &store,
            id,
            Arc::clone(&record_live),
            Arc::clone(&current_generation),
        );
        set_attached(1, true);
        assert!(!store.read(id).unwrap().unwrap().attached);

        set_attached(2, true);
        assert!(store.read(id).unwrap().unwrap().attached);

        *record_live.lock().expect("record_live lock") = false;
        current_generation.store(3, Ordering::SeqCst);
        set_attached(3, false);
        assert!(store.read(id).unwrap().unwrap().attached);
    }
}
