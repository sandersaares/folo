//! On-disk session record.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::AppCommand;
use crate::SessionId;

/// Identity of a supervisor process: pid plus creation time.
///
/// Liveness opens this pid once and verifies the creation time on that handle
/// (design.md, "Session identity"; implementation.md, "Session store").
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ProcessIdentity {
    /// Operating-system process id of the supervisor.
    pub pid: u32,
    /// Process creation time as a Windows `FILETIME` integer, or 0 on tests.
    pub creation_time: u64,
}

/// Persisted description of one live session.
///
/// Written under the PAL store root after the session pipe is accepting
/// (implementation.md, "Session store" and "Process split").
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SessionRecord {
    /// Session id unique among live sessions for this user.
    pub id: u32,
    /// Supervisor process id.
    pub supervisor_pid: u32,
    /// Supervisor process creation time (`FILETIME` as `u64`).
    pub supervisor_creation_time: u64,
    /// Named-pipe path the client connects to.
    pub pipe_name: String,
    /// Canonical absolute launch directory from `dure run`.
    pub launch_directory: PathBuf,
    /// Command argv executed directly, not through a shell.
    pub command: AppCommand,
    /// Unix time in milliseconds when the session was published.
    pub started_at_unix_ms: u64,
    /// Whether the supervisor currently has a client connection.
    #[serde(default)]
    pub attached: bool,
}

/// Contents of one record file.
///
/// An id is claimed before the supervisor has everything a record needs, so the
/// claim names the process that made it. That is what lets a reservation left
/// behind by a supervisor that died mid-initialization be reaped instead of
/// occupying the id forever (implementation.md, "Session store").
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum StoredSession {
    /// The id is claimed and `owner` is still initializing its session.
    Reserved {
        /// Process that claimed the id.
        owner: ProcessIdentity,
    },
    /// The session is published and attachable.
    Published(SessionRecord),
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

impl SessionRecord {
    /// Session id newtype.
    #[must_use]
    pub(crate) fn session_id(&self) -> SessionId {
        SessionId::from_u32(self.id).expect(
            "records are published from allocate_id and rejected on read unless the id is positive",
        )
    }

    /// Supervisor process identity used for liveness and kill.
    #[must_use]
    pub(crate) fn identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            pid: self.supervisor_pid,
            creation_time: self.supervisor_creation_time,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn sample() -> SessionRecord {
        SessionRecord {
            id: 1,
            supervisor_pid: 42,
            supervisor_creation_time: 99,
            pipe_name: r"\\.\pipe\dure-abc".to_string(),
            launch_directory: PathBuf::from(r"C:\work"),
            command: AppCommand::for_test(&["copilot.exe"]),
            started_at_unix_ms: 1,
            attached: false,
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
    fn identity_uses_pid_and_creation_time() {
        let identity = sample().identity();
        assert_eq!(identity.pid, 42);
        assert_eq!(identity.creation_time, 99);
    }

    #[test]
    fn a_reservation_and_a_published_record_are_distinguishable_on_disk() {
        let owner = ProcessIdentity {
            pid: 7,
            creation_time: 8,
        };
        let reserved = StoredSession::Reserved { owner };
        let published = StoredSession::Published(sample());
        let reserved_json = serde_json::to_string(&reserved).unwrap();
        let published_json = serde_json::to_string(&published).unwrap();
        assert_ne!(reserved_json, published_json);
        assert_eq!(
            serde_json::from_str::<StoredSession>(&reserved_json).unwrap(),
            reserved
        );
        assert_eq!(
            serde_json::from_str::<StoredSession>(&published_json).unwrap(),
            published
        );
    }
}
