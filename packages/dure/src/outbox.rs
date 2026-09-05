//! Queued write side of one client connection.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

use crate::constants::MAX_CLIENT_BACKLOG_BYTES;
use crate::pal::ids::ConnId;
use crate::pal::transport::Transport;
use crate::protocol::Message;

/// Everything the supervisor writes to one client, delivered by its own thread.
///
/// A pipe write blocks while the peer is not draining, so writing to a client
/// directly would let a wedged client hold whichever supervisor path made the
/// write: the output pump, the steal that is trying to replace it, or the exit
/// teardown. Queueing here keeps those paths free and confines the block to the
/// thread that owns the connection.
///
/// Messages are written in the order they were queued, which is what keeps
/// `Attached` ahead of output and `AppExited` behind it.
/// Ref: docs/implementation.md, "Transport".
pub(crate) struct Outbox<T: Transport> {
    transport: T,
    conn: ConnId,
    state: Mutex<OutboxState>,
    /// Signals a queued message or a state change.
    changed: Condvar,
    /// Taken by whoever flushes, so the writer is joined exactly once.
    writer: Mutex<Option<thread::JoinHandle<()>>>,
}

/// Queue and lifecycle of one outbox.
#[derive(Debug, Default)]
struct OutboxState {
    queued: VecDeque<Message>,
    /// Payload bytes waiting to be written, the measure of how far behind the
    /// client has fallen.
    pending_bytes: usize,
    /// The connection has been given up: nothing more is queued or written.
    abandoned: bool,
    /// No further messages will be queued; the writer stops once it drains.
    finished: bool,
}

impl<T: Transport + Clone> Outbox<T> {
    /// Starts the writer thread that owns every write to `conn`.
    pub(crate) fn start(transport: T, conn: ConnId) -> Arc<Self> {
        let outbox = Arc::new(Self {
            transport,
            conn,
            state: Mutex::new(OutboxState::default()),
            changed: Condvar::new(),
            writer: Mutex::new(None),
        });
        let writer = thread::spawn({
            let outbox = Arc::clone(&outbox);
            move || outbox.write_loop()
        });
        *outbox
            .writer
            .lock()
            .expect("the writer handle is only taken by a flush, never across a panic") =
            Some(writer);
        outbox
    }

    /// Queues one message. Never blocks.
    ///
    /// A client that falls far enough behind is abandoned instead: `dure` keeps
    /// no screen buffer, so output that cannot be delivered has no later value,
    /// and the user recovers the session with a fresh `dure resume`.
    pub(crate) fn send(&self, message: Message) {
        let overflowed = {
            let mut state = self.lock();
            if state.abandoned || state.finished {
                return;
            }
            state.pending_bytes = state.pending_bytes.saturating_add(payload_len(&message));
            state.queued.push_back(message);
            self.changed.notify_all();
            state.pending_bytes > MAX_CLIENT_BACKLOG_BYTES
        };
        if overflowed {
            self.abandon();
        }
    }

    /// Gives up on the connection, discarding whatever is still queued.
    pub(crate) fn abandon(&self) {
        {
            let mut state = self.lock();
            if state.abandoned {
                return;
            }
            state.abandoned = true;
            state.queued.clear();
            state.pending_bytes = 0;
            self.changed.notify_all();
        }
        // Cancels a write already in flight, which is what releases a writer
        // thread blocked on a client that stopped reading.
        self.transport.disconnect(self.conn);
    }

    /// Stops accepting messages and lets the writer drop the connection once it
    /// has written what is already queued.
    pub(crate) fn finish(&self) {
        let mut state = self.lock();
        state.finished = true;
        self.changed.notify_all();
    }

    /// Waits for the writer to stop and drop the connection.
    ///
    /// This delivers nothing itself: [`Outbox::finish`] or [`Outbox::abandon`]
    /// is what tells the writer to stop, and this only waits for that to have
    /// happened.
    ///
    /// Only the final exit-status delivery waits: by then the session owns no
    /// store record, job, or pseudoconsole, so a client that never drains its
    /// pipe delays nothing beyond this process outliving it.
    pub(crate) fn wait_for_writer(&self) {
        let mut writer = self
            .writer
            .lock()
            .expect("the writer handle is only taken by this wait, never across a panic");
        if let Some(handle) = writer.take() {
            handle
                .join()
                .expect("the outbox writer thread cannot panic");
        }
    }

    fn lock(&self) -> MutexGuard<'_, OutboxState> {
        self.state
            .lock()
            .expect("the outbox lock is only held for queue bookkeeping, never across a panic")
    }

    fn write_loop(&self) {
        while let Some(message) = self.take_next() {
            if self.transport.send(self.conn, &message).is_err() {
                break;
            }
        }
        self.transport.disconnect(self.conn);
    }

    /// Next message to write, or `None` once the outbox is done.
    fn take_next(&self) -> Option<Message> {
        let mut state = self.lock();
        loop {
            if state.abandoned {
                return None;
            }
            if let Some(message) = state.queued.pop_front() {
                state.pending_bytes = state.pending_bytes.saturating_sub(payload_len(&message));
                return Some(message);
            }
            if state.finished {
                return None;
            }
            state = self
                .changed
                .wait(state)
                .expect("the outbox lock is only held for queue bookkeeping, never across a panic");
        }
    }
}

/// Bytes a message contributes to the backlog measure.
fn payload_len(message: &Message) -> usize {
    match message {
        Message::Output(data) | Message::Input(data) => data.len(),
        _ => 0,
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use testing::with_watchdog;

    use super::*;
    use crate::constants::CONNECT_TIMEOUT;
    use crate::pal::transport::MemoryTransport;
    use crate::SessionId;

    /// A connected pair on an in-memory pipe, as supervisor and client ends.
    fn pair() -> (MemoryTransport, ConnId, ConnId) {
        let transport = MemoryTransport::new();
        let name = transport.pipe_name("outbox");
        let listener = transport.listen(&name).unwrap();
        let client = transport.connect(&name, CONNECT_TIMEOUT).unwrap();
        let server = transport.accept(listener).unwrap();
        transport.close_listener(listener);
        (transport, server, client)
    }

    #[test]
    fn queued_messages_arrive_in_order() {
        with_watchdog(|| {
            let (transport, server, client) = pair();
            let outbox = Outbox::start(transport.clone(), server);
            outbox.send(Message::Attached {
                session_id: SessionId::MIN,
            });
            outbox.send(Message::Output(b"hello".to_vec()));
            outbox.send(Message::AppExited { status: 7 });
            outbox.finish();
            outbox.wait_for_writer();

            assert!(matches!(
                transport.recv(client).unwrap(),
                Message::Attached { .. }
            ));
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::Output(b"hello".to_vec())
            );
            assert_eq!(
                transport.recv(client).unwrap(),
                Message::AppExited { status: 7 }
            );
        });
    }

    #[test]
    fn abandoning_drops_the_queue_and_the_connection() {
        with_watchdog(|| {
            let (transport, server, client) = pair();
            let outbox = Outbox::start(transport.clone(), server);
            outbox.abandon();
            outbox.wait_for_writer();
            // Repeat abandonment is how an overflow and an explicit give-up can
            // both land on the same outbox.
            outbox.abandon();
            outbox.send(Message::Output(b"lost".to_vec()));
            transport.recv(client).unwrap_err();
        });
    }

    #[test]
    fn a_client_that_never_drains_is_abandoned_rather_than_blocking_the_sender() {
        with_watchdog(|| {
            let (transport, server, _client) = pair();
            // The peer stops draining its pipe, so every write to it blocks.
            transport.stall(server);
            let outbox = Outbox::start(transport.clone(), server);
            // The backlog cap is the only thing that can end this loop. A
            // blocking send would never return.
            let chunk = vec![0_u8; 64 * 1024];
            // One extra round covers the message the writer already took off
            // the queue and is blocked on.
            let rounds = MAX_CLIENT_BACKLOG_BYTES.div_euclid(chunk.len()) + 2;
            for _ in 0..rounds {
                outbox.send(Message::Output(chunk.clone()));
            }
            outbox.wait_for_writer();
            transport.send(server, &Message::Displaced).unwrap_err();
        });
    }

    #[test]
    fn a_client_within_the_backlog_cap_keeps_everything_queued_for_it() {
        with_watchdog(|| {
            // A burst a responsive client can plausibly fall behind by, and the
            // scale the cap exists to sit well above. Deliberately an absolute
            // size rather than a fraction of the cap, so a cap that no longer
            // clears an ordinary burst fails here.
            const BURST_BYTES: usize = 2 * 1024 * 1024;

            const {
                assert!(
                    BURST_BYTES < MAX_CLIENT_BACKLOG_BYTES,
                    "the cap must leave room for an ordinary burst"
                );
            }

            let (transport, server, client) = pair();
            // The peer stops draining, so the queue accumulates instead of
            // being written out as fast as it is filled.
            transport.stall(server);
            let outbox = Outbox::start(transport.clone(), server);
            let chunk = vec![0_u8; 64 * 1024];
            let rounds = BURST_BYTES.div_euclid(chunk.len());
            for _ in 0..rounds {
                outbox.send(Message::Output(chunk.clone()));
            }
            transport.resume(server);
            outbox.finish();
            outbox.wait_for_writer();

            for _ in 0..rounds {
                assert_eq!(
                    transport.recv(client).unwrap(),
                    Message::Output(chunk.clone())
                );
            }
        });
    }

    #[test]
    fn finishing_stops_accepting_further_messages() {
        with_watchdog(|| {
            let (transport, server, client) = pair();
            let outbox = Outbox::start(transport.clone(), server);
            outbox.finish();
            outbox.send(Message::Output(b"late".to_vec()));
            outbox.wait_for_writer();
            transport.recv(client).unwrap_err();
        });
    }

    #[test]
    fn only_payload_messages_count_towards_the_backlog() {
        assert_eq!(payload_len(&Message::Output(b"abcd".to_vec())), 4);
        assert_eq!(payload_len(&Message::Input(b"ab".to_vec())), 2);
        assert_eq!(payload_len(&Message::Displaced), 0);
        assert_eq!(payload_len(&Message::AppExited { status: 0 }), 0);
    }
}
