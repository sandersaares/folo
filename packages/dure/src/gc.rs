//! Drop session records only when the recorded supervisor process is gone.

use ohno::AppError;

use crate::pal::processes::{ProcessLiveness, Processes};
use crate::pal::session_store::SessionStore;
use crate::session_record::SessionRecord;
use crate::trace::{Trace, trace};
use crate::{InspectProcessError, SessionId, SessionNotFoundError, StoreError};

/// Lists live sessions, deleting records whose supervisor process is gone.
///
/// Id claims left behind by a supervisor that died before publishing are reaped
/// the same way, so a crashed startup does not occupy an id forever.
/// A matching running process is kept even if a later connect times out.
/// Failure to inspect a process is an error and does not delete the record.
pub(crate) fn live_sessions(
    store: &impl SessionStore,
    processes: &impl Processes,
    trace: Trace,
) -> Result<Vec<SessionRecord>, AppError> {
    let records = store.list().map_err(StoreError::caused_by)?;
    trace!(
        trace,
        "read {} session {} from the store",
        records.len(),
        record_noun(records.len())
    );
    let mut live = Vec::new();
    for record in records {
        match processes.probe(&record.supervisor) {
            ProcessLiveness::Live => {
                trace!(
                    trace,
                    "session {}: supervisor pid {} is running, so the session is live",
                    record.id,
                    record.supervisor.pid
                );
                live.push(record);
            }
            ProcessLiveness::Dead => {
                trace!(
                    trace,
                    "session {}: supervisor pid {} is gone, so the record is dropped",
                    record.id,
                    record.supervisor.pid
                );
                // Ids are reused, so deleting by id alone can reap a session
                // that claimed this id since `list` read it.
                store
                    .delete_owned_by(record.id, &record.supervisor)
                    .map_err(StoreError::caused_by)?;
            }
            ProcessLiveness::InspectFailed => {
                trace!(
                    trace,
                    "session {}: supervisor pid {} could not be inspected, so nothing is assumed about it",
                    record.id,
                    record.supervisor.pid
                );
                return Err(InspectProcessError::for_pid(record.supervisor.pid).into());
            }
        }
    }
    reap_orphan_reservations(store, processes, trace)?;
    trace!(trace, "{} live {}", live.len(), session_noun(live.len()));
    Ok(live)
}

// English pluralization is not a behavioral contract.
#[cfg_attr(test, mutants::skip)]
fn record_noun(count: usize) -> &'static str {
    if count == 1 { "record" } else { "records" }
}

// English pluralization is not a behavioral contract.
#[cfg_attr(test, mutants::skip)]
fn session_noun(count: usize) -> &'static str {
    if count == 1 { "session" } else { "sessions" }
}

/// Deletes id claims whose owner is gone.
///
/// An unreadable owner is left alone for the same reason a record is: only a
/// confirmed dead process proves the claim will never be published.
fn reap_orphan_reservations(
    store: &impl SessionStore,
    processes: &impl Processes,
    trace: Trace,
) -> Result<(), AppError> {
    let reservations = store.list_reservations().map_err(StoreError::caused_by)?;
    for (id, owner) in reservations {
        match processes.probe(&owner) {
            ProcessLiveness::Dead => {
                trace!(
                    trace,
                    "id {id} was claimed by pid {} which is gone, so the claim is released",
                    owner.pid
                );
                store
                    .delete_owned_by(id, &owner)
                    .map_err(StoreError::caused_by)?;
            }
            ProcessLiveness::Live => {
                trace!(
                    trace,
                    "id {id} is claimed by pid {} which is still starting up, so the id stays taken",
                    owner.pid
                );
            }
            ProcessLiveness::InspectFailed => {
                trace!(
                    trace,
                    "id {id} is claimed by pid {} which could not be inspected, so the id stays taken",
                    owner.pid
                );
            }
        }
    }
    Ok(())
}

/// Reads and probes only `id`. Unrelated records are not inspected.
pub(crate) fn require_live_session(
    store: &impl SessionStore,
    processes: &impl Processes,
    id: SessionId,
    trace: Trace,
) -> Result<SessionRecord, AppError> {
    let Some(record) = store.read(id).map_err(StoreError::caused_by)? else {
        trace!(trace, "no record for session {id} in the store");
        return Err(SessionNotFoundError::for_id(id).into());
    };
    trace!(
        trace,
        "session {id}: recorded supervisor pid {}, pipe {}",
        record.supervisor.pid,
        record.pipe_name
    );
    match processes.probe(&record.supervisor) {
        ProcessLiveness::Live => Ok(record),
        ProcessLiveness::Dead => {
            trace!(
                trace,
                "session {id}: supervisor pid {} is gone, so the record is dropped",
                record.supervisor.pid
            );
            store
                .delete_owned_by(id, &record.supervisor)
                .map_err(StoreError::caused_by)?;
            Err(SessionNotFoundError::for_id(id).into())
        }
        ProcessLiveness::InspectFailed => {
            Err(InspectProcessError::for_pid(record.supervisor.pid).into())
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::AppCommand;
    use crate::pal::processes::MockProcesses;
    use crate::pal::session_store::{MemorySessionStore, SessionStore};
    use crate::protocol::PROTOCOL_VERSION;
    use crate::session_record::{ProcessIdentity, SessionRecord};

    fn record(id: SessionId, pid: u32, creation: u64) -> SessionRecord {
        SessionRecord {
            id,
            supervisor: ProcessIdentity {
                pid,
                creation_time: creation,
            },
            pipe_name: format!("pipe-{id}"),
            launch_directory: PathBuf::from("/work"),
            command: AppCommand::for_test(&["app.exe"]),
            started_at_unix_ms: 1,
            attached: false,
            protocol_version: PROTOCOL_VERSION,
        }
    }

    #[test]
    fn drops_dead_and_keeps_live() {
        let store = MemorySessionStore::new();
        let live_id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let dead_id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(live_id, 10, 100)).unwrap();
        store.publish(&record(dead_id, 11, 101)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|identity: &ProcessIdentity| {
                if identity.pid == 10 {
                    ProcessLiveness::Live
                } else {
                    ProcessLiveness::Dead
                }
            });

        let live = live_sessions(&store, &processes, Trace::default()).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live.first().expect("one live session").id, live_id);
        assert!(store.read(dead_id).unwrap().is_none());
        assert!(store.read(live_id).unwrap().is_some());
    }

    #[test]
    fn inspect_failure_keeps_record() {
        let store = MemorySessionStore::new();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 10, 100)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::InspectFailed);

        live_sessions(&store, &processes, Trace::default()).unwrap_err();
        assert!(store.read(id).unwrap().is_some());
    }

    #[test]
    fn an_explicit_id_whose_process_cannot_be_inspected_keeps_its_record() {
        let store = MemorySessionStore::new();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 10, 100)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::InspectFailed);

        // Nothing was learned about the supervisor, so nothing is concluded
        // about the session: reaping it here would delete a live one.
        let error = require_live_session(&store, &processes, id, Trace::default()).unwrap_err();
        assert!(error.find_source::<InspectProcessError>().is_some());
        assert!(store.read(id).unwrap().is_some());
    }

    #[test]
    fn require_live_session_does_not_inspect_other_records() {
        let store = MemorySessionStore::new();
        let live_id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let dead_id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(live_id, 10, 100)).unwrap();
        store.publish(&record(dead_id, 11, 101)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .times(1)
            .withf(|identity: &ProcessIdentity| identity.pid == 10)
            .returning(|_| ProcessLiveness::Live);

        let found = require_live_session(&store, &processes, live_id, Trace::default()).unwrap();
        assert_eq!(found.id, live_id);
        assert!(store.read(dead_id).unwrap().is_some());
    }

    #[test]
    fn require_live_session_reaps_a_dead_record() {
        let store = MemorySessionStore::new();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 11, 101)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Dead);

        let error = require_live_session(&store, &processes, id, Trace::default()).unwrap_err();
        assert!(error.find_source::<SessionNotFoundError>().is_some());
        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn reaps_a_reservation_whose_owner_is_gone() {
        let store = MemorySessionStore::new();
        let orphan = store.allocate_id(&ProcessIdentity::for_test(12)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Dead);

        assert!(
            live_sessions(&store, &processes, Trace::default())
                .unwrap()
                .is_empty()
        );
        assert!(store.list_reservations().unwrap().is_empty());
        // The id is free again, so the next claim takes it.
        assert_eq!(
            store.allocate_id(&ProcessIdentity::for_test(13)).unwrap(),
            orphan
        );
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn keeps_a_reservation_whose_owner_is_still_initializing() {
        let store = MemorySessionStore::new();
        let owner = ProcessIdentity::for_test(12);
        let claimed = store.allocate_id(&owner).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);

        assert!(
            live_sessions(&store, &processes, Trace::default())
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.list_reservations().unwrap(), vec![(claimed, owner)]);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn an_unreadable_reservation_owner_is_left_alone() {
        let store = MemorySessionStore::new();
        let owner = ProcessIdentity::for_test(12);
        store.allocate_id(&owner).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::InspectFailed);

        // An unreadable owner is not a confirmed death, so the claim stays and
        // reaping reports success.
        assert!(
            live_sessions(&store, &processes, Trace::default())
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.list_reservations().unwrap().len(), 1);
    }
}
