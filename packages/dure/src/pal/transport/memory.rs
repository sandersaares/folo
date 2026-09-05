//! In-memory transport for unit tests.
//!
//! Accept remains possible while another connection's `recv` is blocked, which
//! is the steal-under-load contract without using the operating system.
//!
//! No wall clock is consulted. A test that wants a bounded wait to expire says
//! so, on the connection or listener it means, and everything else waits until
//! the thing it is waiting for happens or becomes impossible. A regression
//! therefore fails an assertion straight away instead of spending a production
//! timeout first (docs/testing.md, "No real time in tests").

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{ConnId, ListenerId};
use crate::pal::transport::Transport;
use crate::protocol::Message;

struct ConnState {
    incoming: VecDeque<Message>,
    peer: ConnId,
    closed: bool,
    /// Sends on this connection block, standing in for a peer that has stopped
    /// draining its pipe. Tests release them with `resume`; `disconnect` also
    /// releases them, as `CancelIoEx` does.
    stalled: bool,
    /// Senders parked on this connection's stalled predicate.
    ///
    /// Tests observe this under the connection map lock to prove that a
    /// particular send reached the intended blocking boundary.
    stalled_senders: usize,
    /// The pipe this connection belongs to, which is how a test names it.
    pipe: String,
}

struct ListenerState {
    pending: VecDeque<ConnId>,
    /// The pipe this listener serves, which is how a test names it.
    name: String,
}

/// Failures a test has asked this transport to produce, per pipe.
///
/// Injections are held against the pipe a test named rather than against the
/// transport as a whole, so an unrelated accept, receive, or send on another
/// session cannot consume the failure the scenario under test is waiting for.
#[derive(Default)]
struct Faults {
    /// Timed accepts that report an expired wait.
    expire_accepts: usize,
    /// Timed receives that report an expired wait.
    expire_recvs: usize,
    /// Sends that fail without delivering.
    fail_sends: usize,
}

struct Inner {
    next_id: AtomicU64,
    listeners: Mutex<HashMap<String, ListenerId>>,
    listener_state: Mutex<HashMap<ListenerId, ListenerState>>,
    conns: Mutex<HashMap<ConnId, ConnState>>,
    /// Successful startup commits, for client-side protocol assertions.
    startup_commits: AtomicUsize,
    /// Failures tests have asked for, keyed by the pipe they apply to.
    faults: Mutex<HashMap<String, Faults>>,
    /// Guards the pending-connection predicate under `listener_state`. A
    /// `Condvar` may only ever be paired with one mutex, so the connection side
    /// has its own below.
    listener_cond: Condvar,
    /// Guards the queued-message and stalled predicates under `conns`.
    conn_cond: Condvar,
}

/// Thread-safe in-memory named-pipe stand-in.
#[derive(Clone)]
pub(crate) struct MemoryTransport {
    inner: Arc<Inner>,
}

impl fmt::Debug for MemoryTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(MemoryTransport)).finish()
    }
}

impl MemoryTransport {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                listeners: Mutex::new(HashMap::new()),
                listener_state: Mutex::new(HashMap::new()),
                conns: Mutex::new(HashMap::new()),
                startup_commits: AtomicUsize::new(0),
                faults: Mutex::new(HashMap::new()),
                listener_cond: Condvar::new(),
                conn_cond: Condvar::new(),
            }),
        }
    }

    fn alloc_id(&self) -> u64 {
        self.inner.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Make sends on `conn` block until it is disconnected or resumed.
    pub(crate) fn stall(&self, conn: ConnId) {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        let state = conns
            .get_mut(&conn)
            .expect("tests only stall a live connection");
        assert!(!state.stalled, "a connection cannot be stalled twice");
        assert_eq!(
            state.stalled_senders, 0,
            "a connection cannot be rearmed before prior senders resume"
        );
        state.stalled = true;
        self.inner.conn_cond.notify_all();
    }

    /// Let sends on `conn` proceed and wait until its old stall has drained.
    ///
    /// Draining the parked-sender count before returning makes the connection
    /// safe to stall again without observing a sender from the previous stall.
    pub(crate) fn resume(&self, conn: ConnId) {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        let state = conns
            .get_mut(&conn)
            .expect("tests only resume a live connection");
        assert!(state.stalled, "only a stalled connection can be resumed");
        state.stalled = false;
        self.inner.conn_cond.notify_all();
        loop {
            let state = conns
                .get(&conn)
                .expect("connections remain addressable after disconnection");
            if state.stalled_senders == 0 {
                return;
            }
            conns = self.inner.conn_cond.wait(conns).expect("conn condvar");
        }
    }

    /// Block until a send has parked on `conn`'s injected stall.
    pub(crate) fn wait_for_stalled_send(&self, conn: ConnId) {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        loop {
            let state = conns
                .get(&conn)
                .expect("tests only observe a live connection");
            assert!(
                state.stalled,
                "the connection must be stalled before waiting"
            );
            if state.stalled_senders != 0 {
                return;
            }
            conns = self.inner.conn_cond.wait(conns).expect("conn condvar");
        }
    }

    pub(crate) fn startup_commit_count(&self) -> usize {
        self.inner.startup_commits.load(Ordering::SeqCst)
    }

    /// Make the next send on `pipe` fail without delivering its message.
    pub(crate) fn fail_next_send(&self, pipe: &str) {
        self.arm(pipe, |faults| {
            faults.fail_sends = faults.fail_sends.saturating_add(1);
        });
    }

    /// Make the next timed accept on `pipe` report an expired wait.
    pub(crate) fn expire_next_accept(&self, pipe: &str) {
        self.arm(pipe, |faults| {
            faults.expire_accepts = faults.expire_accepts.saturating_add(1);
        });
    }

    /// Make the next timed receive on `pipe` report an expired wait.
    pub(crate) fn expire_next_recv(&self, pipe: &str) {
        self.arm(pipe, |faults| {
            faults.expire_recvs = faults.expire_recvs.saturating_add(1);
        });
    }

    fn arm(&self, pipe: &str, change: impl FnOnce(&mut Faults)) {
        change(
            self.inner
                .faults
                .lock()
                .expect("fault map lock")
                .entry(pipe.to_string())
                .or_default(),
        );
        self.inner.listener_cond.notify_all();
        self.inner.conn_cond.notify_all();
    }

    /// Consumes one armed failure of the kind `take` selects, if any.
    fn take_fault(&self, pipe: &str, take: impl FnOnce(&mut Faults) -> &mut usize) -> bool {
        let mut faults = self.inner.faults.lock().expect("fault map lock");
        let Some(pipe_faults) = faults.get_mut(pipe) else {
            return false;
        };
        let counter = take(pipe_faults);
        if *counter == 0 {
            return false;
        }
        *counter = counter.saturating_sub(1);
        true
    }

    /// `bounded` says whether the caller supplied a deadline, not how long it
    /// is: only an injected expiry ends a bounded wait early here.
    fn accept_inner(&self, listener: ListenerId, bounded: bool) -> Result<ConnId, PalError> {
        let mut state = self
            .inner
            .listener_state
            .lock()
            .expect("listener state lock");
        loop {
            let Some(listener_state) = state.get_mut(&listener) else {
                return Err(PalError::new(PalErrorKind::NotFound));
            };
            if let Some(conn) = listener_state.pending.pop_front() {
                return Ok(conn);
            }
            let pipe = listener_state.name.clone();
            if bounded && self.take_fault(&pipe, |faults| &mut faults.expire_accepts) {
                return Err(PalError::new(PalErrorKind::Timeout));
            }
            state = self
                .inner
                .listener_cond
                .wait(state)
                .expect("listener condvar");
        }
    }

    fn recv_inner(&self, conn: ConnId, bounded: bool) -> Result<Message, PalError> {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        loop {
            let Some(state) = conns.get_mut(&conn) else {
                return Err(PalError::new(PalErrorKind::NotFound));
            };
            if let Some(message) = state.incoming.pop_front() {
                return Ok(message);
            }
            if state.closed {
                return Err(PalError::new(PalErrorKind::Disconnected));
            }
            let pipe = state.pipe.clone();
            if bounded && self.take_fault(&pipe, |faults| &mut faults.expire_recvs) {
                return Err(PalError::new(PalErrorKind::Timeout));
            }
            conns = self.inner.conn_cond.wait(conns).expect("conn condvar");
        }
    }
}

impl Default for MemoryTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for MemoryTransport {
    fn listen(&self, name: &str) -> Result<ListenerId, PalError> {
        let id = ListenerId(self.alloc_id());
        let mut listeners = self.inner.listeners.lock().expect("listener map lock");
        if listeners.contains_key(name) {
            return Err(PalError::new(PalErrorKind::Other));
        }
        listeners.insert(name.to_string(), id);
        self.inner
            .listener_state
            .lock()
            .expect("listener state lock")
            .insert(
                id,
                ListenerState {
                    pending: VecDeque::new(),
                    name: name.to_string(),
                },
            );
        Ok(id)
    }

    fn accept(&self, listener: ListenerId) -> Result<ConnId, PalError> {
        self.accept_inner(listener, false)
    }

    fn accept_timeout(&self, listener: ListenerId, _timeout: Duration) -> Result<ConnId, PalError> {
        self.accept_inner(listener, true)
    }

    fn connect(&self, name: &str, _timeout: Duration) -> Result<ConnId, PalError> {
        let listeners = self.inner.listeners.lock().expect("listener map lock");
        // No listener means nothing to connect to, which is not the same as a
        // wait that was spent: `Timeout` is reserved for a deadline that
        // actually elapsed. Ref: docs/transport.md.
        let Some(&listener) = listeners.get(name) else {
            return Err(PalError::new(PalErrorKind::NotFound));
        };
        drop(listeners);

        let server = ConnId(self.alloc_id());
        let client = ConnId(self.alloc_id());
        {
            let mut conns = self.inner.conns.lock().expect("conn map lock");
            conns.insert(
                server,
                ConnState {
                    incoming: VecDeque::new(),
                    peer: client,
                    closed: false,
                    stalled: false,
                    stalled_senders: 0,
                    pipe: name.to_string(),
                },
            );
            conns.insert(
                client,
                ConnState {
                    incoming: VecDeque::new(),
                    peer: server,
                    closed: false,
                    stalled: false,
                    stalled_senders: 0,
                    pipe: name.to_string(),
                },
            );
        }

        let mut state = self
            .inner
            .listener_state
            .lock()
            .expect("listener state lock");
        let Some(listener_state) = state.get_mut(&listener) else {
            drop(state);
            self.disconnect(server);
            return Err(PalError::new(PalErrorKind::NotFound));
        };
        listener_state.pending.push_back(server);
        self.inner.listener_cond.notify_all();
        Ok(client)
    }

    fn send(&self, conn: ConnId, message: &Message) -> Result<(), PalError> {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        if let Some(state) = conns.get(&conn) {
            let pipe = state.pipe.clone();
            if self.take_fault(&pipe, |faults| &mut faults.fail_sends) {
                return Err(PalError::new(PalErrorKind::Other));
            }
        }
        let mut waiting = false;
        let peer = loop {
            let Some(state) = conns.get_mut(&conn) else {
                return Err(PalError::new(PalErrorKind::NotFound));
            };
            if state.closed {
                if waiting {
                    state.stalled_senders = state.stalled_senders.checked_sub(1).unwrap();
                    self.inner.conn_cond.notify_all();
                }
                return Err(PalError::new(PalErrorKind::NotFound));
            }
            if !state.stalled {
                if waiting {
                    state.stalled_senders = state.stalled_senders.checked_sub(1).unwrap();
                    self.inner.conn_cond.notify_all();
                }
                break state.peer;
            }
            if !waiting {
                state.stalled_senders = state.stalled_senders.checked_add(1).unwrap();
                waiting = true;
                self.inner.conn_cond.notify_all();
            }
            conns = self.inner.conn_cond.wait(conns).expect("conn condvar");
        };
        let Some(state) = conns.get_mut(&peer) else {
            return Err(PalError::new(PalErrorKind::NotFound));
        };
        if state.closed {
            return Err(PalError::new(PalErrorKind::NotFound));
        }
        state.incoming.push_back(message.clone());
        if matches!(message, Message::StartupCommit) {
            self.inner.startup_commits.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.conn_cond.notify_all();
        Ok(())
    }

    fn recv(&self, conn: ConnId) -> Result<Message, PalError> {
        self.recv_inner(conn, false)
    }

    fn recv_timeout(&self, conn: ConnId, _timeout: Duration) -> Result<Message, PalError> {
        self.recv_inner(conn, true)
    }

    fn disconnect(&self, conn: ConnId) {
        let mut conns = self.inner.conns.lock().expect("conn map lock");
        let peer = conns.get(&conn).map(|state| state.peer);
        if let Some(state) = conns.get_mut(&conn) {
            state.closed = true;
        }
        if let Some(peer) = peer
            && let Some(state) = conns.get_mut(&peer)
        {
            state.closed = true;
        }
        self.inner.conn_cond.notify_all();
    }

    fn close_listener(&self, listener: ListenerId) {
        self.inner
            .listeners
            .lock()
            .expect("listener map lock")
            .retain(|_, id| *id != listener);
        let pending = self
            .inner
            .listener_state
            .lock()
            .expect("listener state lock")
            .remove(&listener)
            .map_or_else(VecDeque::new, |state| state.pending);
        self.inner.listener_cond.notify_all();
        for conn in pending {
            self.disconnect(conn);
        }
    }

    fn pipe_name(&self, nonce: &str) -> String {
        format!("memory:{nonce}")
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::thread;

    use testing::with_watchdog;

    use super::*;

    /// A duration this transport never reads; only an armed expiry ends a wait.
    const ANY_TIMEOUT: Duration = Duration::ZERO;

    #[test]
    fn an_armed_accept_expiry_reports_a_timeout() {
        let transport = MemoryTransport::new();
        let listener = transport.listen("session").unwrap();
        transport.expire_next_accept("session");

        let error = transport.accept_timeout(listener, ANY_TIMEOUT).unwrap_err();

        assert_eq!(error.kind(), PalErrorKind::Timeout);
    }

    #[test]
    fn an_armed_receive_expiry_reports_a_timeout() {
        let transport = MemoryTransport::new();
        let listener = transport.listen("session").unwrap();
        let client = transport.connect("session", ANY_TIMEOUT).unwrap();
        _ = transport.accept(listener).unwrap();
        transport.expire_next_recv("session");

        let error = transport.recv_timeout(client, ANY_TIMEOUT).unwrap_err();

        assert_eq!(error.kind(), PalErrorKind::Timeout);
    }

    #[test]
    fn connecting_to_a_pipe_nobody_serves_is_not_a_timeout() {
        // A wait that was never spent must not be reported as one, or the
        // command layer tells the user it exhausted a deadline it did not.
        let error = MemoryTransport::new()
            .connect("nobody", ANY_TIMEOUT)
            .unwrap_err();

        assert_eq!(error.kind(), PalErrorKind::NotFound);
    }

    #[test]
    fn connection_send_stall_reports_each_rearmed_sender() {
        with_watchdog(|| {
            let transport = MemoryTransport::new();
            let listener = transport.listen("session").unwrap();
            let client = transport.connect("session", Duration::ZERO).unwrap();
            let server = transport.accept(listener).unwrap();

            for message in [Message::StartupErr, Message::StartupCommit] {
                transport.stall(client);
                let sender = thread::spawn({
                    let transport = transport.clone();
                    let message = message.clone();
                    move || transport.send(client, &message)
                });

                transport.wait_for_stalled_send(client);
                transport.resume(client);
                sender.join().unwrap().unwrap();
                assert_eq!(transport.recv(server).unwrap(), message);
            }
        });
    }
}
