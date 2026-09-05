//! Filesystem session store.
//!
//! Id allocation uses exclusive file creation so two concurrent `run`
//! invocations cannot take the same id.

// The Win32 record-file mechanics this store is built on: they are meaningful only
// here, so they live under it rather than beside it.
#[cfg_attr(coverage_nightly, coverage(off))]
mod windows;

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::SessionId;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::session_store::SessionStore;
use crate::pal::session_store::fs_store::windows::{RecordFile, move_file_replace};
use crate::pal::session_store::stored::StoredSession;
use crate::session_record::{ProcessIdentity, SessionRecord};

/// Session store rooted at a caller-supplied directory.
#[derive(Clone, Debug)]
pub(crate) struct FsSessionStore {
    root: PathBuf,
}

fn parse_stored(bytes: &[u8]) -> Result<StoredSession, PalError> {
    serde_json::from_slice(bytes).map_err(|error| {
        PalError::with_source(
            PalErrorKind::Other,
            io::Error::new(io::ErrorKind::InvalidData, error),
        )
    })
}

fn parse_record(bytes: &[u8], expected: SessionId) -> Result<Option<SessionRecord>, PalError> {
    let StoredSession::Published(record) = parse_stored(bytes)? else {
        return Ok(None);
    };
    if record.id != expected {
        return Ok(None);
    }
    Ok(Some(record))
}

fn replace_file(tmp: &Path, dest: &Path) -> io::Result<()> {
    move_file_replace(tmp, dest)
}

impl FsSessionStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn record_path(&self, id: SessionId) -> PathBuf {
        self.root.join(format!("{}.json", id.get()))
    }

    /// Every readable record file, paired with the id its name encodes.
    ///
    /// Foreign, torn, and unparseable files are skipped: one bad file must not
    /// hide every session in the store from `dure list`.
    ///
    /// A file that names a session but cannot be read is different: it is a
    /// session the store knows about and cannot report. That is an error, so a
    /// permission or sharing problem shows up as one rather than as an empty
    /// session list. Ref: docs/session-store.md.
    fn stored(&self) -> Result<Vec<(SessionId, StoredSession)>, PalError> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(PalError::from_io(error)),
        };
        let mut stored = Vec::new();
        for entry in entries {
            let entry = entry.map_err(PalError::from_io)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(raw) = stem.parse::<u32>() else {
                continue;
            };
            let Some(id) = SessionId::from_u32(raw) else {
                continue;
            };
            let bytes = match fs::read(entry.path()) {
                Ok(bytes) => bytes,
                // A file that disappeared between the listing and the read is
                // a session that ended, which is the store working.
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(PalError::from_io(error)),
            };
            let Ok(parsed) = parse_stored(&bytes) else {
                continue;
            };
            stored.push((id, parsed));
        }
        stored.sort_by_key(|(id, _stored)| id.get());
        Ok(stored)
    }
}

impl SessionStore for FsSessionStore {
    #[cfg(test)]
    fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn allocate_id(&self, owner: &ProcessIdentity) -> Result<SessionId, PalError> {
        fs::create_dir_all(&self.root).map_err(PalError::from_io)?;
        let claim = serde_json::to_vec(&StoredSession::Reserved { owner: *owner }).map_err(
            // A reservation is a fixed struct of integers, so the only way
            // serialization reports a failure is a defect in `serde_json`
            // itself. Nothing can drive this arm from a test.
            #[cfg_attr(coverage_nightly, coverage(off))]
            |error| {
                PalError::with_source(
                    PalErrorKind::Other,
                    io::Error::new(io::ErrorKind::InvalidData, error),
                )
            },
        )?;
        let mut n: u32 = 1;
        loop {
            let Some(id) = SessionId::from_u32(n) else {
                return Err(PalError::new(PalErrorKind::Other));
            };
            let path = self.record_path(id);
            // Exclusive create of `{id}.json` is the claim, and the claim names
            // the process making it. `read` and `list` report a claimed id as
            // absent, while `gc` reaps one whose owner died before publishing.
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    return write_claim(&mut file, &claim).map_or_else(
                        |error| {
                            // A claim nobody owns would occupy the id forever.
                            drop(file);
                            _ = fs::remove_file(&path);
                            Err(PalError::from_io(error))
                        },
                        |()| Ok(id),
                    );
                }
                Err(error) => {
                    if !is_id_taken(&error) {
                        return Err(PalError::from_io(error));
                    }
                    n = n
                        .checked_add(1)
                        .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
                }
            }
        }
    }

    fn publish(&self, record: &SessionRecord) -> Result<(), PalError> {
        fs::create_dir_all(&self.root).map_err(PalError::from_io)?;
        let id = record.id;
        let path = self.record_path(id);
        let tmp = self.root.join(format!("{}.json.tmp", id.get()));
        let json = serde_json::to_vec_pretty(&StoredSession::Published(record.clone())).map_err(
            // A record is a fixed struct of strings, integers, and paths, so
            // the only way serialization reports a failure is a defect in
            // `serde_json` itself. Nothing can drive this arm from a test.
            #[cfg_attr(coverage_nightly, coverage(off))]
            |error| {
                PalError::with_source(
                    PalErrorKind::Other,
                    io::Error::new(io::ErrorKind::InvalidData, error),
                )
            },
        )?;
        let mut file = File::create(&tmp).map_err(PalError::from_io)?;
        file.write_all(&json).map_err(PalError::from_io)?;
        file.sync_all().map_err(PalError::from_io)?;
        drop(file);
        replace_file(&tmp, &path).map_err(PalError::from_io)
    }

    fn read(&self, id: SessionId) -> Result<Option<SessionRecord>, PalError> {
        let path = self.record_path(id);
        match fs::read(&path) {
            Ok(bytes) if bytes.is_empty() => Ok(None),
            Ok(bytes) => parse_record(&bytes, id),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(PalError::from_io(error)),
        }
    }

    fn list(&self) -> Result<Vec<SessionRecord>, PalError> {
        Ok(self
            .stored()?
            .into_iter()
            .filter_map(|(id, stored)| match stored {
                StoredSession::Published(record) if record.id == id => Some(record),
                _ => None,
            })
            .collect())
    }

    fn list_reservations(&self) -> Result<Vec<(SessionId, ProcessIdentity)>, PalError> {
        Ok(self
            .stored()?
            .into_iter()
            .filter_map(|(id, stored)| match stored {
                StoredSession::Reserved { owner } => Some((id, owner)),
                StoredSession::Published(_record) => None,
            })
            .collect())
    }

    fn delete_owned_by(&self, id: SessionId, owner: &ProcessIdentity) -> Result<(), PalError> {
        // Reading and deleting through one handle is what makes this safe to race: ids are
        // reused, so between deciding and deleting the name can come to mean a live session
        // somebody else just published. The handle addresses the file the decision was made
        // about, so such a replacement is never the file removed.
        let file = match RecordFile::open(&self.record_path(id)) {
            Ok(file) => file,
            Err(error) if is_absent(&error) => return Ok(()),
            Err(error) => return Err(PalError::from_io(error)),
        };
        let bytes = file.read().map_err(PalError::from_io)?;
        // A file that no longer names `owner` belongs to whoever claimed the id
        // after the caller read it, and a file nothing can parse names nobody.
        let owned = match parse_stored(&bytes) {
            Ok(StoredSession::Reserved { owner: current }) => current == *owner,
            Ok(StoredSession::Published(record)) => record.supervisor == *owner,
            Err(_error) => false,
        };
        if !owned {
            return Ok(());
        }
        file.delete().map_err(PalError::from_io)
    }

    fn canonicalize(&self, path: &Path) -> Result<PathBuf, PalError> {
        fs::canonicalize(path).map_err(PalError::from_io)
    }

    fn current_dir(&self) -> Result<PathBuf, PalError> {
        env::current_dir().map_err(PalError::from_io)
    }
}

/// Whether a failure to open a record means the name simply is not there.
///
/// Nothing to delete is not a failure to delete, so an id whose file is already gone is a
/// caller asking for a state that already holds. Any other open failure is a real fault.
fn is_absent(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::NotFound)
}

/// Whether an exclusive-create failure means the id is already reserved.
///
/// Any other failure is a filesystem fault: treating it as a taken id would
/// retry the same fault against every remaining id in turn.
fn is_id_taken(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::AlreadyExists
}

/// Writes and flushes a claim so it is durable before the id is handed out.
fn write_claim(file: &mut File, claim: &[u8]) -> io::Result<()> {
    file.write_all(claim)?;
    file.sync_all()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;
    use std::{iter, thread};

    use tempfile::TempDir;
    use testing::with_watchdog;

    use super::*;
    use crate::AppCommand;

    fn store() -> (TempDir, FsSessionStore) {
        let dir = TempDir::new().unwrap();
        let store = FsSessionStore::new(dir.path().to_path_buf());
        (dir, store)
    }

    fn record(id: SessionId, dir: &Path) -> SessionRecord {
        SessionRecord {
            id,
            supervisor: ProcessIdentity {
                pid: 1,
                creation_time: 1,
            },
            pipe_name: "pipe".to_string(),
            launch_directory: dir.to_path_buf(),
            command: AppCommand::for_test(&["app.exe"]),
            started_at_unix_ms: 1,
            attached: false,
        }
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_claim_names_its_owner_until_it_is_published() {
        let (dir, store) = store();
        let owner = ProcessIdentity::for_test(7);
        let id = store.allocate_id(&owner).unwrap();

        // A claimed id is not yet a session, so it lists as a reservation only.
        assert_eq!(store.list_reservations().unwrap(), vec![(id, owner)]);
        assert!(store.list().unwrap().is_empty());
        assert!(store.read(id).unwrap().is_none());

        store.publish(&record(id, dir.path())).unwrap();
        assert!(store.list_reservations().unwrap().is_empty());
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_claim_is_deleted_only_by_its_owner() {
        let (_dir, store) = store();
        let owner = ProcessIdentity::for_test(7);
        let id = store.allocate_id(&owner).unwrap();

        store
            .delete_owned_by(id, &ProcessIdentity::for_test(8))
            .unwrap();
        assert_eq!(store.list_reservations().unwrap().len(), 1);

        store.delete_owned_by(id, &owner).unwrap();
        assert!(store.list_reservations().unwrap().is_empty());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_is_deleted_only_by_its_own_supervisor() {
        let (dir, store) = store();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let mut published = record(id, dir.path());
        published.supervisor = ProcessIdentity {
            pid: 20,
            creation_time: 200,
        };
        store.publish(&published).unwrap();

        // Stands in for a session that took this id after another process read
        // the record it means to delete.
        let stale = ProcessIdentity {
            pid: 20,
            creation_time: 199,
        };
        store.delete_owned_by(id, &stale).unwrap();
        assert_eq!(store.read(id).unwrap().unwrap(), published);

        store.delete_owned_by(id, &published.supervisor).unwrap();
        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_file_that_is_still_empty_reads_as_absent() {
        let (dir, store) = store();
        let id = SessionId::from_u32(3).unwrap();
        // What a claim looks like between its file being created and its owner
        // being written into it.
        fs::write(dir.path().join(format!("{}.json", id.get())), b"").unwrap();

        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_that_cannot_be_inspected_is_not_deleted() {
        let (dir, store) = store();
        let id = SessionId::from_u32(3).unwrap();
        // Stands in for any record this process cannot read. Failing to inspect
        // one is not evidence that it may be removed.
        fs::create_dir_all(dir.path().join(format!("{}.json", id.get()))).unwrap();

        store
            .delete_owned_by(id, &ProcessIdentity::for_test(7))
            .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn deleting_what_is_not_there_or_not_readable_is_not_an_error() {
        let (dir, store) = store();
        let owner = ProcessIdentity::for_test(7);
        let id = store.allocate_id(&owner).unwrap();
        // A file nothing can parse names nobody, so no owner may delete it.
        fs::write(dir.path().join(format!("{}.json", id.get())), b"not json").unwrap();
        store.delete_owned_by(id, &owner).unwrap();
        assert!(dir.path().join(format!("{}.json", id.get())).exists());

        store
            .delete_owned_by(SessionId::from_u32(99).unwrap(), &owner)
            .unwrap();
    }

    #[test]
    fn only_an_already_exists_failure_means_the_id_is_taken() {
        assert!(is_id_taken(&io::Error::from(io::ErrorKind::AlreadyExists)));
        assert!(!is_id_taken(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn allocates_smallest_unused_and_reuses_after_delete() {
        let (dir, store) = store();
        assert_eq!(store.root(), dir.path());
        let first = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let second = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        assert_eq!(first.get(), 1);
        assert_eq!(second.get(), 2);
        store
            .delete_owned_by(first, &ProcessIdentity::for_test(1))
            .unwrap();
        let reused = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        assert_eq!(reused.get(), 1);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_reserved_but_unpublished_id_reads_as_absent() {
        let (_dir, store) = store();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        assert!(store.read(id).unwrap().is_none());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn concurrent_allocations_are_unique() {
        with_watchdog(|| {
            let dir = TempDir::new().unwrap();
            let root = dir.path().to_path_buf();
            let threads: Vec<_> = iter::repeat_with(|| {
                let root = root.clone();
                thread::spawn(move || {
                    let store = FsSessionStore::new(root);
                    store.allocate_id(&ProcessIdentity::for_test(1)).unwrap()
                })
            })
            .take(8)
            .collect();
            let mut ids = HashSet::new();
            for handle in threads {
                assert!(ids.insert(handle.join().unwrap().get()));
            }
            assert_eq!(ids.len(), 8);
        });
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn publish_read_list_delete() {
        let (dir, store) = store();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let rec = record(id, dir.path());
        store.publish(&rec).unwrap();
        assert_eq!(store.read(id).unwrap().unwrap(), rec);
        assert_eq!(store.list().unwrap(), vec![rec.clone()]);
        store.delete_owned_by(id, &rec.supervisor).unwrap();
        assert!(store.read(id).unwrap().is_none());
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn list_skips_files_that_are_not_usable_records() {
        let (dir, store) = store();
        fs::write(dir.path().join("0.json"), b"{\"id\":0}").unwrap();
        fs::write(dir.path().join("not-json.json"), b"nope").unwrap();
        fs::write(dir.path().join("readme.txt"), b"not a record").unwrap();
        // Names a valid id but holds nothing that parses as a session.
        fs::write(dir.path().join("5.json"), b"nope").unwrap();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let rec = record(id, dir.path());
        store.publish(&rec).unwrap();
        assert_eq!(store.list().unwrap(), vec![rec]);
        assert!(store.list_reservations().unwrap().is_empty());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_the_store_cannot_read_is_reported_rather_than_omitted() {
        let (dir, store) = store();
        // Names a valid session but cannot be read. Omitting it would make an
        // unreadable store look like an empty one.
        fs::create_dir_all(dir.path().join("6.json")).unwrap();

        store.list().unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn read_reports_a_corrupt_record() {
        let (dir, store) = store();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        fs::write(dir.path().join(format!("{}.json", id.get())), b"nope").unwrap();
        assert_eq!(store.read(id).unwrap_err().kind(), PalErrorKind::Other);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_path_that_is_not_a_file_is_an_error() {
        let (dir, store) = store();
        let id = SessionId::MIN;
        fs::create_dir_all(dir.path().join(format!("{}.json", id.get()))).unwrap();
        store.read(id).unwrap_err();
        store
            .delete_owned_by(id, &ProcessIdentity::for_test(1))
            .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_root_that_is_not_a_directory_is_an_error() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("store");
        fs::write(&root, b"not a directory").unwrap();
        let store = FsSessionStore::new(root);

        store.list().unwrap_err();
        store.list_reservations().unwrap_err();
        store
            .allocate_id(&ProcessIdentity::for_test(1))
            .unwrap_err();
        store
            .publish(&record(SessionId::MIN, dir.path()))
            .unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_staging_path_that_is_not_a_file_fails_the_publish() {
        let (dir, store) = store();
        let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        // Occupies the name `publish` stages the new record under.
        fs::create_dir_all(dir.path().join(format!("{}.json.tmp", id.get()))).unwrap();

        store.publish(&record(id, dir.path())).unwrap_err();
        // The claim survives, so the id is still this owner's to publish under.
        assert_eq!(store.list_reservations().unwrap().len(), 1);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_too_large_to_read_in_one_go_is_still_its_owner_s_to_delete() {
        let (dir, store) = store();
        let owner = ProcessIdentity::for_test(1);
        let id = store.allocate_id(&owner).unwrap();
        // A launch directory and a command line are as long as the user made them, and Windows
        // permits command lines far longer than any convenient buffer size.
        let mut record = record(id, &dir.path().join("d".repeat(9_000)));
        record.command = AppCommand::from_argv(vec!["app.exe".to_string(), "a".repeat(20_000)])
            .expect("test argv names an executable");
        store.publish(&record).unwrap();

        // A record read short would parse as nobody's and be declined, stranding the id.
        store.delete_owned_by(id, &record.supervisor).unwrap();
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn deleting_an_absent_record_succeeds() {
        let (_dir, store) = store();
        store
            .delete_owned_by(SessionId::MIN, &ProcessIdentity::for_test(1))
            .unwrap();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn a_record_that_replaced_the_one_inspected_survives_the_delete() {
        let (dir, store) = store();
        let owner = ProcessIdentity::for_test(1);
        let id = store.allocate_id(&owner).unwrap();
        let path = dir.path().join(format!("{}.json", id.get()));

        // Stands in for the record the caller decided to remove, held open so the decision and
        // the removal are separated by exactly the window the ids-are-reused race needs.
        let inspected = RecordFile::open(&path).unwrap();
        // Whoever held the id lets it go and someone else takes it, all under the same name.
        fs::remove_file(&path).unwrap();
        let successor = record(id, dir.path());
        store.publish(&successor).unwrap();

        inspected.delete().unwrap();
        drop(inspected);

        // The delete landed on the file it inspected, not on the name it was reached through.
        assert_eq!(store.list().unwrap().len(), 1);
        // And the successor can still be found and removed by the owner it names.
        store.delete_owned_by(id, &successor.supervisor).unwrap();
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn canonicalize_resolves_an_existing_path_and_rejects_a_missing_one() {
        let (dir, store) = store();
        let resolved = store.canonicalize(dir.path()).unwrap();
        assert!(resolved.is_absolute());
        assert_eq!(resolved, fs::canonicalize(dir.path()).unwrap());
        store.canonicalize(&dir.path().join("absent")).unwrap_err();
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn read_rejects_filename_id_mismatch() {
        let (dir, store) = store();
        let first = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        let rec = record(first, dir.path());
        store.publish(&rec).unwrap();
        let second = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
        fs::write(
            dir.path().join(format!("{}.json", second.get())),
            serde_json::to_vec(&StoredSession::Published(rec.clone())).unwrap(),
        )
        .unwrap();
        assert!(store.read(second).unwrap().is_none());
        assert_eq!(store.list().unwrap(), vec![rec.clone()]);
        store.delete_owned_by(second, &rec.supervisor).unwrap();
        assert_eq!(store.read(first).unwrap().unwrap(), rec);
    }

    #[test]
    // Talks to the real operating system: the session store is a real directory.
    #[cfg_attr(miri, ignore)]
    fn missing_root_lists_and_reads_empty() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("no-such-store");
        let store = FsSessionStore::new(missing);
        assert!(store.list().unwrap().is_empty());
        assert!(store.read(SessionId::MIN).unwrap().is_none());
    }
}
