//! Carrying the console between the app and whichever client owns it.
//!
//! Ref: docs/supervisor.md, "Relay".

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use ohno::AppError;

use crate::constants::CONNECT_TIMEOUT;
use crate::outbox::Outbox;
use crate::pal::ids::{ConnId, ListenerId};
use crate::pal::processes::Processes;
use crate::pal::pseudoconsole::Pseudoconsole;
use crate::pal::session_store::SessionStore;
use crate::pal::transport::Transport;
use crate::protocol::Message;
use crate::supervisor::record_writer::RecordWriter;
use crate::supervisor::shared::{Client, FirstAttach, Shared, preamble_messages};
use crate::supervisor::startup::Initialized;
use crate::{PalFailedError, StoreError};

// Blocking serve loop. A mutation that returns before the accept and PTY threads
// are wired up leaves the test's client waiting forever, and watchdogs are
// disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
pub(super) fn serve<P, S, T, C>(
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
    let &Initialized {
        session_id,
        identity,
        listener,
        pty,
        job,
        app,
        ..
    } = initialized;

    let attached_generation = Arc::new(AtomicU64::default());
    let record_writer = RecordWriter::start(store, session_id, Arc::clone(&attached_generation));
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

    let store_flag = record_writer.set_attached();
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

    // Nothing can publish the record after this, so the delete below is final
    // — including over a session id that is later reused.
    record_writer.finish();
    // Ids are reused, so an unconditional delete could reap whichever session
    // claimed this id after this supervisor published.
    let deleted = store.delete_owned_by(session_id, &identity);

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

// Blocking accept. A mutation that drops the stop check or the accept error
// path hangs unit tests because watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
pub(super) fn accept_loop<T, C>(
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
pub(super) fn client_loop<T, C>(
    shared: &Shared<T, C>,
    conn: ConnId,
    set_attached: &impl Fn(u64, bool),
) where
    T: Transport + Clone,
    C: Pseudoconsole,
{
    match shared.transport.recv_timeout(conn, CONNECT_TIMEOUT) {
        Ok(Message::Attach { size }) => {
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
            _ = shared.pty_host.resize(shared.pty, size);
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
            Message::Resize { size } => {
                _ = shared.pty_host.resize(shared.pty, size);
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
pub(super) fn pty_output_loop<T, C>(shared: &Shared<T, C>)
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
