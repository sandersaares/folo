//! Framed client-supervisor protocol.
//!
//! The named pipe carries these messages (docs/transport.md).

// Framing is used by the Windows named-pipe transport. Unit tests cover it on
// every target; the non-Windows lib build has no transport that serializes.
#![cfg_attr(
    not(any(windows, test)),
    expect(
        dead_code,
        reason = "named-pipe framing is Windows transport plus tests"
    )
)]

use std::mem::size_of;

use crate::SessionId;
use crate::constants::MAX_FRAME_LEN;
use crate::durability::LauncherTie;

/// One framed message on the client-supervisor pipe or the startup channel.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum Message {
    /// Client is attaching and reports its console size.
    Attach {
        /// Console width in columns.
        cols: u16,
        /// Console height in rows.
        rows: u16,
    },
    /// Console input bytes from the client to the app.
    Input(Vec<u8>),
    /// Client console size changed while attached.
    Resize {
        /// Console width in columns.
        cols: u16,
        /// Console height in rows.
        rows: u16,
    },
    /// Supervisor accepted this client as the sole live console.
    Attached {
        /// Session this client is now attached to.
        session_id: SessionId,
    },
    /// Console output bytes from the app to the client.
    Output(Vec<u8>),
    /// This client has been displaced by a newer attach.
    Displaced,
    /// The app has exited.
    AppExited {
        /// Process exit status of the app.
        status: i32,
    },
    /// Supervisor finished initializing and is accepting attaches.
    StartupOk {
        /// Newly published session id.
        session_id: SessionId,
        /// Whether the session can outlive the client that started it.
        launcher_tie: LauncherTie,
    },
    /// Supervisor initialization failed.
    StartupErr,
    /// Client received startup confirmation and accepts ownership of the session.
    StartupCommit,
}

// Launcher-tie bytes are stable assigned integers, like the kind bytes below.
const LAUNCHER_TIE_NONE_DETECTED: u8 = 1;
const LAUNCHER_TIE_CONFIRMED: u8 = 2;
const LAUNCHER_TIE_UNKNOWN: u8 = 3;

// Kind bytes are stable assigned integers. New kinds take the next unused
// value. Retired kinds are never reused.
const KIND_ATTACH: u8 = 1;
const KIND_INPUT: u8 = 2;
const KIND_RESIZE: u8 = 3;
const KIND_ATTACHED: u8 = 4;
const KIND_OUTPUT: u8 = 5;
const KIND_DISPLACED: u8 = 6;
const KIND_APP_EXITED: u8 = 7;
const KIND_STARTUP_OK: u8 = 8;
const KIND_STARTUP_ERR: u8 = 9;
const KIND_STARTUP_COMMIT: u8 = 10;

/// Encodes a message as a length-prefixed frame.
#[must_use]
pub(crate) fn encode(message: &Message) -> Vec<u8> {
    let mut payload = Vec::new();
    match message {
        Message::Attach { cols, rows } => {
            payload.push(KIND_ATTACH);
            payload.extend_from_slice(&cols.to_le_bytes());
            payload.extend_from_slice(&rows.to_le_bytes());
        }
        Message::Input(data) => {
            payload.push(KIND_INPUT);
            payload.extend_from_slice(data);
        }
        Message::Resize { cols, rows } => {
            payload.push(KIND_RESIZE);
            payload.extend_from_slice(&cols.to_le_bytes());
            payload.extend_from_slice(&rows.to_le_bytes());
        }
        Message::Attached { session_id } => {
            payload.push(KIND_ATTACHED);
            payload.extend_from_slice(&session_id.get().to_le_bytes());
        }
        Message::Output(data) => {
            payload.push(KIND_OUTPUT);
            payload.extend_from_slice(data);
        }
        Message::Displaced => payload.push(KIND_DISPLACED),
        Message::AppExited { status } => {
            payload.push(KIND_APP_EXITED);
            payload.extend_from_slice(&status.to_le_bytes());
        }
        Message::StartupOk {
            session_id,
            launcher_tie,
        } => {
            payload.push(KIND_STARTUP_OK);
            payload.extend_from_slice(&session_id.get().to_le_bytes());
            payload.push(match *launcher_tie {
                LauncherTie::NoneDetected => LAUNCHER_TIE_NONE_DETECTED,
                LauncherTie::Confirmed => LAUNCHER_TIE_CONFIRMED,
                LauncherTie::Unknown => LAUNCHER_TIE_UNKNOWN,
            });
        }
        Message::StartupErr => payload.push(KIND_STARTUP_ERR),
        Message::StartupCommit => payload.push(KIND_STARTUP_COMMIT),
    }

    let len = u32::try_from(payload.len()).expect("frame payload fits in u32");
    let mut frame = Vec::with_capacity(
        size_of::<u32>()
            .checked_add(payload.len())
            .expect("frame length fits in usize"),
    );
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Failure while decoding a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeError {
    /// Bytes do not form a complete well-typed message.
    Invalid,
}

/// Decodes one length-prefixed frame. `data` is the payload after the length word.
pub(crate) fn decode_payload(payload: &[u8]) -> Result<Message, DecodeError> {
    let Some((kind, rest)) = payload.split_first() else {
        return Err(DecodeError::Invalid);
    };
    match *kind {
        KIND_ATTACH => decode_size(rest).map(|(cols, rows)| Message::Attach { cols, rows }),
        KIND_INPUT => Ok(Message::Input(rest.to_vec())),
        KIND_RESIZE => decode_size(rest).map(|(cols, rows)| Message::Resize { cols, rows }),
        KIND_ATTACHED => decode_session_id(rest).map(|session_id| Message::Attached { session_id }),
        KIND_OUTPUT => Ok(Message::Output(rest.to_vec())),
        KIND_DISPLACED if rest.is_empty() => Ok(Message::Displaced),
        KIND_APP_EXITED => decode_i32(rest).map(|status| Message::AppExited { status }),
        KIND_STARTUP_OK => {
            let Some((launcher_tie, id_bytes)) = rest.split_last() else {
                return Err(DecodeError::Invalid);
            };
            let launcher_tie = match *launcher_tie {
                LAUNCHER_TIE_NONE_DETECTED => LauncherTie::NoneDetected,
                LAUNCHER_TIE_CONFIRMED => LauncherTie::Confirmed,
                LAUNCHER_TIE_UNKNOWN => LauncherTie::Unknown,
                _ => return Err(DecodeError::Invalid),
            };
            decode_session_id(id_bytes).map(|session_id| Message::StartupOk {
                session_id,
                launcher_tie,
            })
        }
        KIND_STARTUP_ERR if rest.is_empty() => Ok(Message::StartupErr),
        KIND_STARTUP_COMMIT if rest.is_empty() => Ok(Message::StartupCommit),
        _ => Err(DecodeError::Invalid),
    }
}

/// Returns whether a declared payload length is within the sanity cap.
#[must_use]
pub(crate) fn payload_len_ok(len: u32) -> bool {
    len > 0 && len <= MAX_FRAME_LEN
}

fn decode_size(rest: &[u8]) -> Result<(u16, u16), DecodeError> {
    let (cols_bytes, rest) = rest.split_at_checked(2).ok_or(DecodeError::Invalid)?;
    let (rows_bytes, rest) = rest.split_at_checked(2).ok_or(DecodeError::Invalid)?;
    if !rest.is_empty() {
        return Err(DecodeError::Invalid);
    }
    let cols = u16::from_le_bytes(
        cols_bytes
            .try_into()
            .map_err(|_error| DecodeError::Invalid)?,
    );
    let rows = u16::from_le_bytes(
        rows_bytes
            .try_into()
            .map_err(|_error| DecodeError::Invalid)?,
    );
    Ok((cols, rows))
}

fn decode_u32(rest: &[u8]) -> Result<u32, DecodeError> {
    let (bytes, rest) = rest.split_at_checked(4).ok_or(DecodeError::Invalid)?;
    if !rest.is_empty() {
        return Err(DecodeError::Invalid);
    }
    Ok(u32::from_le_bytes(
        bytes.try_into().map_err(|_error| DecodeError::Invalid)?,
    ))
}

fn decode_session_id(rest: &[u8]) -> Result<SessionId, DecodeError> {
    SessionId::from_u32(decode_u32(rest)?).ok_or(DecodeError::Invalid)
}

fn decode_i32(rest: &[u8]) -> Result<i32, DecodeError> {
    Ok(i32::from_le_bytes(decode_u32(rest)?.to_le_bytes()))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn round_trips_each_kind() {
        let id = SessionId::MIN;
        let messages = [
            Message::Attach { cols: 80, rows: 24 },
            Message::Input(b"hi".to_vec()),
            Message::Resize {
                cols: 120,
                rows: 30,
            },
            Message::Attached { session_id: id },
            Message::Output(b"out".to_vec()),
            Message::Displaced,
            Message::AppExited { status: 7 },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::NoneDetected,
            },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::Confirmed,
            },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::Unknown,
            },
            Message::StartupErr,
            Message::StartupCommit,
        ];
        for message in messages {
            let frame = encode(&message);
            let (header, payload) = frame
                .split_first_chunk::<4>()
                .expect("encode always writes a length prefix");
            let len = u32::from_le_bytes(*header);
            assert!(payload_len_ok(len));
            assert_eq!(payload.len(), len as usize);
            assert_eq!(decode_payload(payload).unwrap(), message);
        }
    }

    #[test]
    fn rejects_empty_payload() {
        assert_eq!(decode_payload(&[]).unwrap_err(), DecodeError::Invalid);
    }

    #[test]
    fn rejects_zero_session_id() {
        let mut payload = vec![KIND_ATTACHED];
        payload.extend_from_slice(&0_u32.to_le_bytes());
        assert_eq!(decode_payload(&payload).unwrap_err(), DecodeError::Invalid);
    }

    #[test]
    fn payload_len_rejects_zero_and_over_cap() {
        assert!(!payload_len_ok(0));
        assert!(!payload_len_ok(MAX_FRAME_LEN.saturating_add(1)));
        assert!(payload_len_ok(1));
        assert!(payload_len_ok(64 * 1024));
        assert!(payload_len_ok(MAX_FRAME_LEN));
    }

    #[test]
    fn empty_messages_reject_trailing_bytes() {
        assert_eq!(
            decode_payload(&[KIND_DISPLACED, 0]).unwrap_err(),
            DecodeError::Invalid
        );
        assert_eq!(
            decode_payload(&[KIND_STARTUP_ERR, 1]).unwrap_err(),
            DecodeError::Invalid
        );
        assert_eq!(
            decode_payload(&[KIND_STARTUP_COMMIT, 1]).unwrap_err(),
            DecodeError::Invalid
        );
    }

    #[test]
    fn startup_ok_rejects_a_missing_or_unassigned_launcher_tie() {
        let id = SessionId::MIN;
        let mut without_tie = vec![KIND_STARTUP_OK];
        without_tie.extend_from_slice(&id.get().to_le_bytes());
        assert_eq!(
            decode_payload(&without_tie).unwrap_err(),
            DecodeError::Invalid
        );
        let mut unassigned_tie = without_tie.clone();
        unassigned_tie.push(0);
        assert_eq!(
            decode_payload(&unassigned_tie).unwrap_err(),
            DecodeError::Invalid
        );
        assert_eq!(
            decode_payload(&[KIND_STARTUP_OK]).unwrap_err(),
            DecodeError::Invalid
        );
    }

    #[test]
    fn sized_and_numeric_payloads_reject_trailing_bytes() {
        let mut attach = vec![KIND_ATTACH];
        attach.extend_from_slice(&80_u16.to_le_bytes());
        attach.extend_from_slice(&24_u16.to_le_bytes());
        attach.push(0);
        assert_eq!(decode_payload(&attach).unwrap_err(), DecodeError::Invalid);

        let mut attached = vec![KIND_ATTACHED];
        attached.extend_from_slice(&1_u32.to_le_bytes());
        attached.push(0);
        assert_eq!(decode_payload(&attached).unwrap_err(), DecodeError::Invalid);
    }
}
