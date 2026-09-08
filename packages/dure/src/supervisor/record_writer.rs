//! The one thing that changes a published session record while it lives.
//!
//! Ref: docs/supervisor.md, "The advisory attached flag".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use crate::SessionId;
use crate::pal::session_store::SessionStore;

/// Owns every change to a live session's record, in one place, in order.
///
/// The attached flag is advisory bookkeeping, but publishing it means
/// serializing a record, writing a file, and replacing it. Doing that on the
/// relay would make a newly attached client's first keystrokes wait for
/// filesystem I/O, and doing it under a lock teardown also wants would make
/// deleting the record wait for the same. One writer holds both problems: the
/// relay hands over an update and moves on, and the delete is simply the last
/// thing this writer does.
///
/// Ordering is what makes the delete final. An update queued before it is
/// applied first, and one queued after cannot exist, because the delete
/// consumes the writer.
pub(crate) struct RecordWriter {
    updates: Sender<Command>,
    worker: JoinHandle<()>,
}

/// What the writer is being asked to do.
enum Command {
    /// Publish this attached state, if it is still the current one.
    SetAttached { generation: u64, attached: bool },
    /// Stop; the record is about to be deleted.
    Stop,
}

impl RecordWriter {
    pub(crate) fn start<S: SessionStore + Clone + Send + 'static>(
        store: &S,
        id: SessionId,
        current_generation: Arc<AtomicU64>,
    ) -> Self {
        let (updates, queue) = mpsc::channel();
        let worker = thread::spawn({
            let store = store.clone();
            move || publish_updates(&store, id, &current_generation, &queue)
        });
        Self { updates, worker }
    }

    /// Hands over an attached-flag change without waiting for it.
    ///
    /// The change is published only while it is still the current ownership
    /// state, so an update that waited behind store I/O cannot overwrite a
    /// newer attach or detach.
    pub(crate) fn set_attached(&self) -> impl Fn(u64, bool) + Clone + Send + 'static {
        let updates = self.updates.clone();
        move |generation: u64, attached: bool| {
            // A stopped writer means the record is already being deleted,
            // which is exactly when this update no longer means anything.
            _ = updates.send(Command::SetAttached {
                generation,
                attached,
            });
        }
    }

    /// Waits for every update queued so far, and applies no more.
    ///
    /// Relay threads can outlive teardown still holding the means to queue an
    /// update, so stopping is an instruction rather than the queue closing.
    /// Everything handed over before it is published; everything after it is
    /// dropped. The record can then be deleted knowing nothing will publish it
    /// again — including over a session id that has since been reused.
    pub(crate) fn finish(self) {
        // A worker that already ended leaves nothing to wait for.
        _ = self.updates.send(Command::Stop);
        // The worker only ever publishes an already-live record, so this waits
        // for at most the updates that were queued, not for anything a client
        // may still be doing.
        _ = self.worker.join();
    }
}

// Blocking queue drain. A mutation that stops draining leaves a deterministic
// store-stall test waiting for an operation that can never arrive, and
// watchdogs are disabled under cargo-mutants.
#[cfg_attr(test, mutants::skip)]
fn publish_updates<S: SessionStore>(
    store: &S,
    id: SessionId,
    current_generation: &AtomicU64,
    queue: &Receiver<Command>,
) {
    while let Ok(Command::SetAttached {
        generation,
        attached,
    }) = queue.recv()
    {
        if current_generation.load(Ordering::SeqCst) != generation {
            continue;
        }
        if let Ok(Some(mut record)) = store.read(id) {
            record.attached = attached;
            // A failed write leaves a stale attached flag. The flag is
            // advisory; liveness is the supervisor process.
            _ = store.publish(&record);
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::AppCommand;
    use crate::pal::session_store::MemorySessionStore;
    use crate::protocol::PROTOCOL_VERSION;
    use crate::session_record::{ProcessIdentity, SessionRecord};

    /// A published session for the writer to update.
    fn published() -> (MemorySessionStore, SessionId, ProcessIdentity) {
        let store = MemorySessionStore::new();
        let owner = ProcessIdentity::for_test(1);
        let id = store.allocate_id(&owner).unwrap();
        store
            .publish(&SessionRecord {
                id,
                supervisor: owner,
                pipe_name: "pipe".to_string(),
                launch_directory: PathBuf::from("/work"),
                command: AppCommand::for_test(&["app.exe"]),
                started_at_unix_ms: 1,
                attached: false,
                protocol_version: PROTOCOL_VERSION,
            })
            .unwrap();
        (store, id, owner)
    }

    #[test]
    fn an_update_that_is_still_current_is_published() {
        let (store, id, _owner) = published();
        let generation = Arc::new(AtomicU64::new(1));
        let writer = RecordWriter::start(&store, id, Arc::clone(&generation));

        writer.set_attached()(1, true);
        writer.finish();

        assert!(store.read(id).unwrap().unwrap().attached);
    }

    #[test]
    fn an_update_a_newer_one_has_overtaken_is_discarded() {
        let (store, id, _owner) = published();
        let generation = Arc::new(AtomicU64::new(2));
        let writer = RecordWriter::start(&store, id, Arc::clone(&generation));

        // What an attach that waited behind store I/O would hand over: the
        // ownership state it saw has since been replaced.
        writer.set_attached()(1, true);
        writer.finish();

        assert!(!store.read(id).unwrap().unwrap().attached);
    }

    #[test]
    fn nothing_a_client_left_behind_republishes_a_deleted_record() {
        let (store, id, owner) = published();
        let generation = Arc::new(AtomicU64::new(1));
        let writer = RecordWriter::start(&store, id, Arc::clone(&generation));
        let leftover = writer.set_attached();

        writer.finish();
        store.delete_owned_by(id, &owner).unwrap();
        // A relay thread that outlived teardown, still holding its update.
        leftover(1, true);

        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    fn teardown_waits_for_an_update_that_is_already_being_written() {
        testing::with_watchdog(|| {
            let (store, id, _owner) = published();
            let generation = Arc::new(AtomicU64::new(1));
            let writer = RecordWriter::start(&store, id, Arc::clone(&generation));

            store.stall_publishes();
            writer.set_attached()(1, true);
            store.wait_for_stalled_publish();
            let finished = thread::spawn(move || writer.finish());
            store.resume_publishes();
            finished.join().unwrap();

            // Finishing after the in-flight write is what makes a delete that
            // follows it final.
            assert!(store.read(id).unwrap().unwrap().attached);
        });
    }
}
