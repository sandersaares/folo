//! In-memory transport for unit tests.
//!
//! Accept remains possible while another connection's `recv` is blocked, which
//! is the steal-under-load contract without using the operating system.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
}

struct ListenerState {
    pending: VecDeque<ConnId>,
}

struct Inner {
    next_id: AtomicU64,
    listeners: Mutex<HashMap<String, ListenerId>>,
    listener_state: Mutex<HashMap<ListenerId, ListenerState>>,
    conns: Mutex<HashMap<ConnId, ConnState>>,
    /// Successful startup commits, for client-side protocol assertions.
    startup_commits: AtomicUsize,
    /// Makes the next send fail so callers can exercise transport-failure handling.
    fail_next_send: AtomicBool,
    /// Makes the next accept report a timeout without waiting.
    timeout_next_accept: AtomicBool,
    /// Makes the next timed receive report a timeout once its connection is idle.
    timeout_next_recv: AtomicBool,
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
                fail_next_send: AtomicBool::new(false),
                timeout_next_accept: AtomicBool::new(false),
                timeout_next_recv: AtomicBool::new(false),
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

    /// Make the next send fail without delivering its message.
    pub(crate) fn fail_next_send(&self) {
        self.inner.fail_next_send.store(true, Ordering::SeqCst);
    }

    pub(crate) fn timeout_next_accept(&self) {
        self.inner.timeout_next_accept.store(true, Ordering::SeqCst);
    }

    pub(crate) fn timeout_next_recv(&self) {
        let _conns = self.inner.conns.lock().expect("conn map lock");
        self.inner.timeout_next_recv.store(true, Ordering::SeqCst);
        self.inner.conn_cond.notify_all();
    }

    fn accept_inner(
        &self,
        listener: ListenerId,
        timeout: Option<Duration>,
    ) -> Result<ConnId, PalError> {
        if self.inner.timeout_next_accept.swap(false, Ordering::SeqCst) {
            return Err(PalError::new(PalErrorKind::Timeout));
        }
        let started = Instant::now();
        let mut state = self
            .inner
            .listener_state
            .lock()
            .expect("listener state lock");
        loop {
            if let Some(listener_state) = state.get_mut(&listener)
                && let Some(conn) = listener_state.pending.pop_front()
            {
                return Ok(conn);
            }
            if !state.contains_key(&listener) {
                return Err(PalError::new(PalErrorKind::NotFound));
            }
            state = if let Some(timeout) = timeout {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(PalError::new(PalErrorKind::Timeout));
                }
                self.inner
                    .listener_cond
                    .wait_timeout(state, remaining)
                    .expect("listener condvar")
                    .0
            } else {
                self.inner
                    .listener_cond
                    .wait(state)
                    .expect("listener condvar")
            };
        }
    }

    fn recv_inner(&self, conn: ConnId, timeout: Option<Duration>) -> Result<Message, PalError> {
        let started = Instant::now();
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
            if timeout.is_some() && self.inner.timeout_next_recv.swap(false, Ordering::SeqCst) {
                return Err(PalError::new(PalErrorKind::Timeout));
            }
            conns = if let Some(timeout) = timeout {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(PalError::new(PalErrorKind::Timeout));
                }
                self.inner
                    .conn_cond
                    .wait_timeout(conns, remaining)
                    .expect("conn condvar")
                    .0
            } else {
                self.inner.conn_cond.wait(conns).expect("conn condvar")
            };
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
                },
            );
        Ok(id)
    }

    fn accept(&self, listener: ListenerId) -> Result<ConnId, PalError> {
        self.accept_inner(listener, None)
    }

    fn accept_timeout(&self, listener: ListenerId, timeout: Duration) -> Result<ConnId, PalError> {
        self.accept_inner(listener, Some(timeout))
    }

    fn connect(&self, name: &str, _timeout: Duration) -> Result<ConnId, PalError> {
        let listeners = self.inner.listeners.lock().expect("listener map lock");
        let Some(&listener) = listeners.get(name) else {
            return Err(PalError::new(PalErrorKind::Timeout));
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
        if self.inner.fail_next_send.swap(false, Ordering::SeqCst) {
            return Err(PalError::new(PalErrorKind::Other));
        }
        let mut conns = self.inner.conns.lock().expect("conn map lock");
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
        self.recv_inner(conn, None)
    }

    fn recv_timeout(&self, conn: ConnId, timeout: Duration) -> Result<Message, PalError> {
        self.recv_inner(conn, Some(timeout))
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

    #[test]
    fn accept_timeout_returns_timeout_when_no_connection_is_pending() {
        let transport = MemoryTransport::new();
        let listener = transport.listen("session").unwrap();

        let error = transport
            .accept_timeout(listener, Duration::ZERO)
            .unwrap_err();

        assert_eq!(error.kind(), PalErrorKind::Timeout);
    }

    #[test]
    fn recv_timeout_returns_timeout_when_no_message_is_pending() {
        let transport = MemoryTransport::new();
        let listener = transport.listen("session").unwrap();
        let client = transport.connect("session", Duration::ZERO).unwrap();
        _ = transport.accept(listener).unwrap();

        let error = transport.recv_timeout(client, Duration::ZERO).unwrap_err();

        assert_eq!(error.kind(), PalErrorKind::Timeout);
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
