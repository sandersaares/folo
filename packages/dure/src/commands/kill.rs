//! `dure kill`.

use ohno::AppError;

use crate::gc::require_live_session;
use crate::pal::error::PalErrorKind;
use crate::pal::processes::Processes;
use crate::pal::session_store::SessionStore;
use crate::trace::{Trace, trace};
use crate::{KillFailedError, SessionId, SessionNotFoundError, StoreError};

/// Abruptly terminate the recorded supervisor process.
pub(crate) fn execute(
    store: &impl SessionStore,
    processes: &impl Processes,
    id: SessionId,
    trace: Trace,
) -> Result<(), AppError> {
    let record = require_live_session(store, processes, id, trace)?;
    let identity = record.supervisor;
    trace!(
        trace,
        "terminating supervisor pid {}, which ends the app it owns", identity.pid
    );
    match processes.terminate(&identity) {
        Ok(()) => {}
        Err(error) if error.kind() == PalErrorKind::NotFound => {
            // The supervisor exited between the liveness probe and this
            // terminate. There is nothing left to kill, but the record it left
            // behind is this command's to reap before it reports that the
            // session is not live. Ref: docs/design.md, "Commands".
            trace!(
                trace,
                "supervisor pid {} was already gone; reaping its record", identity.pid
            );
            store
                .delete_owned_by(id, &identity)
                .map_err(StoreError::caused_by)?;
            return Err(SessionNotFoundError::for_id(id).into());
        }
        Err(_error) => return Err(KillFailedError::for_id(id).into()),
    }
    trace!(trace, "removing the record for session {id}");
    store
        .delete_owned_by(id, &identity)
        .map_err(StoreError::caused_by)?;
    Ok(())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;
    use crate::AppCommand;
    use crate::pal::error::PalError;
    use crate::pal::processes::{MockProcesses, ProcessLiveness};
    use crate::pal::session_store::{FsSessionStore, SessionStore};
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
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn missing_id_fails() {
        let dir = TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let mut processes = MockProcesses::new();
        processes.expect_probe().never();
        let id = SessionId::from_u32(1).unwrap();
        execute(&store, &processes, id, Trace::default()).unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn terminates_recorded_identity_and_deletes() {
        let dir = TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 10, 100)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);
        processes
            .expect_terminate()
            .withf(|identity: &ProcessIdentity| identity.pid == 10 && identity.creation_time == 100)
            .returning(|_| Ok(()));

        execute(&store, &processes, id, Trace::default()).unwrap();
        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_supervisor_that_exits_first_is_reaped_and_reported_as_not_live() {
        let dir = TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 10, 100)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);
        processes
            .expect_terminate()
            .returning(|_| Err(PalError::new(PalErrorKind::NotFound)));

        let error = execute(&store, &processes, id, Trace::default()).unwrap_err();
        assert!(error.find_source::<SessionNotFoundError>().is_some());
        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_survives_a_terminate_that_failed_for_another_reason() {
        let dir = TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        store.publish(&record(id, 10, 100)).unwrap();

        let mut processes = MockProcesses::new();
        processes
            .expect_probe()
            .returning(|_| ProcessLiveness::Live);
        processes
            .expect_terminate()
            .returning(|_| Err(PalError::new(PalErrorKind::Other)));

        let error = execute(&store, &processes, id, Trace::default()).unwrap_err();
        assert!(error.find_source::<KillFailedError>().is_some());
        assert!(store.read(id).unwrap().is_some());
    }
}
