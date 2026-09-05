//! The session metadata the whole crate works with.

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{AppCommand, SessionId};

/// Identity of a supervisor process: pid plus creation time.
///
/// The pair is one value because either half alone is ambiguous: a pid is
/// reused, and a creation time names nothing on its own. Liveness opens the pid
/// once and verifies the creation time on that handle, so a record carrying
/// halves of two different processes would be an identity that never existed.
/// Ref: design.md, "Session identity"; docs/session-store.md.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ProcessIdentity {
    /// Operating-system process id of the supervisor.
    pub pid: u32,
    /// Process creation time as a Windows `FILETIME` integer, or 0 on tests.
    pub creation_time: u64,
}

/// Description of one live session.
///
/// Published by the supervisor once its session pipe is accepting, and read by
/// every command that works with sessions.
///
/// The typed fields are the point. A session id is positive and an owner is a
/// process, so a record cannot exist that names a session that could not be or
/// an owner that never was — and the decisions made from these fields, whether
/// a session is live and whether its record may be removed, would otherwise act
/// on such a record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "StoredRecord", into = "StoredRecord")]
pub(crate) struct SessionRecord {
    /// Session id, unique among live sessions for this user.
    pub id: SessionId,
    /// The supervisor process that owns this session.
    pub supervisor: ProcessIdentity,
    /// Named-pipe path the client connects to.
    pub pipe_name: String,
    /// Canonical absolute launch directory from `dure run`.
    pub launch_directory: PathBuf,
    /// Command executed directly, not through a shell.
    pub command: AppCommand,
    /// Unix time in milliseconds when the session was published.
    pub started_at_unix_ms: u64,
    /// Whether the supervisor currently has a client connection.
    ///
    /// Advisory. It is published after ownership has already transferred and a
    /// failed write is tolerated, so it is what `list` shows rather than
    /// something any decision is made from.
    pub attached: bool,
    /// The wire format this session's supervisor speaks.
    ///
    /// Recorded so a client from a different build can refuse the session and
    /// say why, rather than connecting and failing on a frame it cannot read.
    /// Ref: docs/transport.md.
    pub protocol_version: u32,
}

/// The shape a session record has on disk.
///
/// Kept separate from [`SessionRecord`] so the in-memory model can carry typed
/// values while the file keeps the flat fields it has always had. A record
/// outlives the process that wrote it, so the file shape is a contract even
/// though the model above it is free to change.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredRecord {
    id: u32,
    supervisor_pid: u32,
    supervisor_creation_time: u64,
    pipe_name: String,
    launch_directory: PathBuf,
    command: AppCommand,
    started_at_unix_ms: u64,
    #[serde(default)]
    attached: bool,
    /// Absent in records written before versioning existed. Those describe the
    /// format as it stands, so they read as the version that introduced the
    /// field rather than as a mismatch.
    #[serde(default = "first_protocol_version")]
    protocol_version: u32,
}

const fn first_protocol_version() -> u32 {
    1
}

/// Why a stored record could not become a [`SessionRecord`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MalformedRecord;

impl fmt::Display for MalformedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a session record names a positive session id")
    }
}

impl TryFrom<StoredRecord> for SessionRecord {
    type Error = MalformedRecord;

    fn try_from(stored: StoredRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            // Validated here rather than trusted, because a torn or hand-edited
            // file reaches this point and every later reader assumes the id is
            // one a session could have had.
            id: SessionId::from_u32(stored.id).ok_or(MalformedRecord)?,
            supervisor: ProcessIdentity {
                pid: stored.supervisor_pid,
                creation_time: stored.supervisor_creation_time,
            },
            pipe_name: stored.pipe_name,
            launch_directory: stored.launch_directory,
            command: stored.command,
            started_at_unix_ms: stored.started_at_unix_ms,
            attached: stored.attached,
            protocol_version: stored.protocol_version,
        })
    }
}

impl From<SessionRecord> for StoredRecord {
    fn from(record: SessionRecord) -> Self {
        Self {
            id: record.id.get(),
            supervisor_pid: record.supervisor.pid,
            supervisor_creation_time: record.supervisor.creation_time,
            pipe_name: record.pipe_name,
            launch_directory: record.launch_directory,
            command: record.command,
            started_at_unix_ms: record.started_at_unix_ms,
            attached: record.attached,
            protocol_version: record.protocol_version,
        }
    }
}

impl ProcessIdentity {
    /// Identity of an arbitrary process, for tests that only need identities
    /// that compare equal or unequal to each other.
    #[cfg(test)]
    pub(crate) fn for_test(pid: u32) -> Self {
        Self {
            pid,
            creation_time: 0,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION;

    fn sample() -> SessionRecord {
        SessionRecord {
            id: SessionId::MIN,
            supervisor: ProcessIdentity {
                pid: 42,
                creation_time: 99,
            },
            pipe_name: r"\\.\pipe\dure-abc".to_string(),
            launch_directory: PathBuf::from(r"C:\work"),
            command: AppCommand::for_test(&["copilot.exe"]),
            started_at_unix_ms: 1,
            attached: false,
            protocol_version: PROTOCOL_VERSION,
        }
    }

    #[test]
    fn round_trips_json() {
        let record = sample();
        let json = serde_json::to_string(&record).unwrap();
        let parsed: SessionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn the_stored_shape_is_what_a_later_dure_will_read() {
        // Records outlive the process that wrote them, and a `dure` that has
        // been upgraded under a running session still has to read them. The
        // literal is here so that a change to the field names or nesting is a
        // decision someone makes rather than a rename that happens to compile.
        assert_eq!(
            serde_json::to_string(&sample()).unwrap(),
            concat!(
                r#"{"id":1,"supervisor_pid":42,"supervisor_creation_time":99,"#,
                r#""pipe_name":"\\\\.\\pipe\\dure-abc","launch_directory":"C:\\work","#,
                r#""command":["copilot.exe"],"started_at_unix_ms":1,"attached":false,"#,
                r#""protocol_version":1}"#,
            )
        );
    }

    #[test]
    fn a_record_written_before_versioning_reads_as_the_first_version() {
        // Such a record describes the format as it stands, so treating it as a
        // mismatch would refuse sessions that work.
        let without_version = concat!(
            r#"{"id":1,"supervisor_pid":42,"supervisor_creation_time":99,"#,
            r#""pipe_name":"p","launch_directory":"C:\\work","#,
            r#""command":["copilot.exe"],"started_at_unix_ms":1,"attached":false}"#,
        );
        let record = serde_json::from_str::<SessionRecord>(without_version).unwrap();
        assert_eq!(record.protocol_version, 1);
    }

    #[test]
    fn a_record_written_before_the_attached_flag_existed_still_reads() {
        // The flag is advisory and was added after the format existed, so its
        // absence means "not known to be attached" rather than a broken record.
        let without_flag = concat!(
            r#"{"id":1,"supervisor_pid":42,"supervisor_creation_time":99,"#,
            r#""pipe_name":"p","launch_directory":"C:\\work","#,
            r#""command":["copilot.exe"],"started_at_unix_ms":1}"#,
        );
        let record = serde_json::from_str::<SessionRecord>(without_flag).unwrap();
        assert!(!record.attached);
    }

    #[test]
    fn a_record_naming_an_impossible_session_is_refused() {
        // Zero is not an id any session ever had, so a file claiming it is
        // rejected where it is read rather than trusted until something later
        // tries to use it.
        let zero_id = concat!(
            r#"{"id":0,"supervisor_pid":42,"supervisor_creation_time":99,"#,
            r#""pipe_name":"p","launch_directory":"C:\\work","#,
            r#""command":["copilot.exe"],"started_at_unix_ms":1}"#,
        );
        let error = serde_json::from_str::<SessionRecord>(zero_id).unwrap_err();
        // Serde renders the refusal, so a reason that says nothing leaves the
        // user with a file they cannot act on.
        assert!(
            error.to_string().contains("positive session id"),
            "the refusal must say what is wrong with the file, got {error}"
        );
    }
}
