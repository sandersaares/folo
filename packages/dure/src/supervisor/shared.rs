//! What the supervisor's threads coordinate through.
//!
//! Ref: docs/supervisor.md, "Shared state".

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::SessionId;
use crate::constants::{MAX_CLIENT_BACKLOG_BYTES, MAX_OUTPUT_CHUNK_BYTES};
use crate::outbox::Outbox;
use crate::pal::ids::{ConnId, PtyId};
use crate::pal::transport::Transport;
use crate::protocol::Message;

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
pub(super) struct Shared<T: Transport, C> {
    pub(super) transport: T,
    pub(super) pty_host: C,
    pub(super) pty: PtyId,
    pub(super) session_id: SessionId,
    /// Holding this across a pseudoconsole write keeps a displaced client from
    /// reaching the app between its ownership check and the write itself.
    /// Writes here land in the console host's input buffer, which the host
    /// drains independently of the app, so the hold is bounded. Attach also
    /// acknowledges under it, which orders `Attached` ahead of any `Output` on
    /// the same connection and closes the window in which output would be
    /// discarded for want of an installed client.
    pub(super) client: Mutex<Option<Client<T>>>,
    /// Serializes an entire attach: acknowledgement, ownership transfer, and
    /// displacement of the previous client. Without it two attaches can
    /// acknowledge in one order and install in another, letting an older
    /// attach displace a newer one.
    pub(super) attach: Mutex<()>,
    /// Monotonic identity of the latest client-slot ownership state.
    ///
    /// Advisory store updates carry the generation assigned under the client
    /// slot, so an older update that waited for store I/O cannot overwrite a
    /// newer attach or detach.
    pub(super) attached_generation: Arc<AtomicU64>,
    /// Output the app produced before anyone attached, kept for the first
    /// client. Taken under the client slot, which is what orders it ahead of
    /// the output that follows the attach.
    pub(super) preamble: Mutex<Option<Vec<u8>>>,
    /// The supervisor's first-attach lifetime gate.
    ///
    /// Holds an exited app's session open until a client has attached or the
    /// initiating startup connection has closed.
    pub(super) first_attach: Mutex<FirstAttach>,
    pub(super) first_attach_changed: Condvar,
    pub(super) stopping: AtomicBool,
}

/// The connection that currently owns the console, and its write side.
pub(super) struct Client<T: Transport> {
    pub(super) conn: ConnId,
    pub(super) outbox: Arc<Outbox<T>>,
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
pub(super) struct FirstAttach {
    /// Whether a client has ever taken the session.
    ///
    /// Never cleared: this records that the gate opened, not that a client is
    /// attached now. A client that later detaches leaves it set, because the
    /// session it was waiting for has already been claimed once.
    pub(super) claimed: bool,
    /// Whether the process that started the session dropped its channel.
    pub(super) initiator_gone: bool,
}

impl<T: Transport, C> Shared<T, C> {
    pub(super) fn first_attach(&self) -> MutexGuard<'_, FirstAttach> {
        self.first_attach
            .lock()
            .expect("first-attach flags are only set, never held across a panic")
    }

    /// Records that a client took the session.
    // A mutation that drops either flag or the wait leaves the gate closed, and
    // watchdogs are disabled under cargo-mutants, so the test hangs instead of
    // failing.
    #[cfg_attr(test, mutants::skip)]
    pub(super) fn note_claimed(&self) {
        self.first_attach().claimed = true;
        self.first_attach_changed.notify_all();
    }

    /// Records that the process that started the session dropped its channel.
    #[cfg_attr(test, mutants::skip)]
    pub(super) fn note_initiator_gone(&self) {
        self.first_attach().initiator_gone = true;
        self.first_attach_changed.notify_all();
    }

    /// Blocks until the first client has attached or the initiating startup
    /// connection has closed.
    #[cfg_attr(test, mutants::skip)]
    pub(super) fn await_first_attach(&self) {
        let mut state = self.first_attach();
        while !state.claimed && !state.initiator_gone {
            state = self
                .first_attach_changed
                .wait(state)
                .expect("first-attach flags are only set, never held across a panic");
        }
    }

    pub(super) fn client(&self) -> MutexGuard<'_, Option<Client<T>>> {
        self.client
            .lock()
            .expect("client slot is only copied or replaced, never held across a panic")
    }

    /// Advances the identity of the client-slot ownership state.
    // A constant-return mutation makes later ownership updates indistinguishable.
    // The deterministic stall test then waits for an update that is correctly
    // discarded, and mutation watchdogs are disabled.
    #[cfg_attr(test, mutants::skip)]
    pub(super) fn next_attached_generation(&self) -> u64 {
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
    pub(super) fn hold_for_first_client(&self, bytes: &[u8]) {
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
    pub(super) fn take_preamble(&self) -> Option<Vec<u8>> {
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
pub(super) fn preamble_messages(held: &[u8]) -> impl Iterator<Item = Message> + use<'_> {
    held.chunks(MAX_OUTPUT_CHUNK_BYTES.get())
        .map(|chunk| Message::Output(chunk.to_vec()))
}
