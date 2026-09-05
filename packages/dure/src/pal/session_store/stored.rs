//! How a session id's state is encoded where it is stored.
//!
//! An id passes through two states, and both live in one place so they cannot
//! disagree: a *claim*, taken before the supervisor has everything a session
//! needs, and a *published session*. A claim names the process that made it,
//! which is what lets one left behind by a supervisor that died
//! mid-initialization be reaped instead of occupying the id forever.
//!
//! This is a persistence detail of the store implementations. Nothing above the
//! session-store PAL reasons about claims; commands work with published
//! sessions. Ref: docs/session-store.md.

use serde::{Deserialize, Serialize};

use crate::session_record::{ProcessIdentity, SessionRecord};

/// What one stored id says about itself.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum StoredSession {
    /// The id is claimed and `owner` is still initializing its session.
    Reserved {
        /// Process that claimed the id.
        owner: ProcessIdentity,
    },
    /// The session is published and attachable.
    Published(SessionRecord),
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{AppCommand, SessionId};

    fn record() -> SessionRecord {
        SessionRecord {
            id: SessionId::MIN,
            supervisor: ProcessIdentity {
                pid: 42,
                creation_time: 99,
            },
            pipe_name: "p".to_string(),
            launch_directory: PathBuf::from(r"C:\work"),
            command: AppCommand::for_test(&["copilot.exe"]),
            started_at_unix_ms: 1,
            attached: false,
        }
    }

    #[test]
    fn a_claim_stores_the_owner_it_names() {
        // Without the owner, a claim left by a dead supervisor could never be
        // proved abandoned, and the id would stay taken for the logon session.
        let stored = StoredSession::Reserved {
            owner: ProcessIdentity {
                pid: 7,
                creation_time: 8,
            },
        };
        assert_eq!(
            serde_json::to_string(&stored).unwrap(),
            r#"{"kind":"reserved","owner":{"pid":7,"creation_time":8}}"#
        );
    }

    #[test]
    fn a_claim_and_a_published_session_are_distinguishable_on_disk() {
        let reserved = StoredSession::Reserved {
            owner: ProcessIdentity::for_test(7),
        };
        let published = StoredSession::Published(record());
        for stored in [reserved, published] {
            let json = serde_json::to_string(&stored).unwrap();
            assert_eq!(
                serde_json::from_str::<StoredSession>(&json).unwrap(),
                stored
            );
        }
    }
}
