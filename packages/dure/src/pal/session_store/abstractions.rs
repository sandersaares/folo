//! Session store PAL: records, id allocation, and path canonicalization.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::SessionId;
use crate::pal::error::PalError;
use crate::session_record::{ProcessIdentity, SessionRecord};

/// Per-user filesystem of live-session records.
///
/// The real implementation uses per-user `LocalAppData`, subdirectory `dure`.
/// Tests supply an isolated root instead of the user's store.
/// Ref: docs/implementation.md, "PAL slicing"; docs/session-store.md.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait SessionStore: Send + Sync + fmt::Debug + 'static {
    /// Directory that holds session record files.
    #[cfg(test)]
    fn root(&self) -> PathBuf;

    /// Smallest unused positive integer, filesystem-coordinated.
    ///
    /// The claim names `owner` so that a reservation whose owner died before
    /// publishing can be reaped rather than occupying the id forever.
    fn allocate_id(&self, owner: &ProcessIdentity) -> Result<SessionId, PalError>;

    /// Overwrites the record file for this id.
    fn publish(&self, record: &SessionRecord) -> Result<(), PalError>;

    /// Reads one record, or `None` if the file is absent or still reserved.
    fn read(&self, id: SessionId) -> Result<Option<SessionRecord>, PalError>;

    /// Every parseable record in the store, including stale ones.
    fn list(&self) -> Result<Vec<SessionRecord>, PalError>;

    /// Ids that are claimed but not published, with the process that claimed them.
    fn list_reservations(&self) -> Result<Vec<(SessionId, ProcessIdentity)>, PalError>;

    /// Removes the record file only while it still describes `owner`.
    ///
    /// Ids are reused, so an unconditional delete can reap a session that
    /// happens to have claimed the id since the caller last read it. Missing
    /// files succeed.
    fn delete_owned_by(&self, id: SessionId, owner: &ProcessIdentity) -> Result<(), PalError>;

    /// Canonical absolute form used as the launch-directory key.
    fn canonicalize(&self, path: &Path) -> Result<PathBuf, PalError>;

    /// Current working directory of this process.
    fn current_dir(&self) -> Result<PathBuf, PalError>;
}
