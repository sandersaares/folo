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
use crate::pal::pseudoconsole::WindowSize;

/// The wire format this build speaks.
///
/// A supervisor outlives the terminal that started it, so a `dure` upgraded in
/// the meantime can meet a supervisor from the previous build. Cross-build
/// resume is not supported: the message layouts below are free to change, and
/// pretending otherwise would turn a layout change into a corrupt frame rather
/// than a clear refusal.
///
/// A supervisor records the version it speaks when it publishes its session, so
/// a client can refuse before it connects and say why. Bump this whenever a
/// message layout changes or a kind byte's meaning changes.
///
/// Ref: docs/transport.md; docs/design.md, "Attach, detach, steal".
pub(crate) const PROTOCOL_VERSION: u32 = 1;

/// One framed message on the client-supervisor pipe or the startup channel.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum Message {
    /// Client is attaching and reports its console size.
    Attach {
        /// Console the client is attaching from.
        size: WindowSize,
    },
    /// Console input bytes from the client to the app.
    Input(Vec<u8>),
    /// Client console size changed while attached.
    Resize {
        /// Size the console now has.
        size: WindowSize,
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
        /// Pipe the initiating client attaches on, so first attach does not
        /// have to read back the record the supervisor just wrote.
        pipe_name: String,
    },
    /// Supervisor initialization failed.
    StartupErr {
        /// Where startup stopped, so the client can name the subsystem.
        step: StartupStep,
    },
    /// Client received startup confirmation and accepts ownership of the session.
    StartupCommit,
}

/// The step of supervisor startup that a failure stopped at.
///
/// The supervisor runs without a console, so this is what lets the client
/// name the subsystem that failed rather than reporting that something did.
/// Ref: docs/supervisor.md, "Startup".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupStep {
    /// Creating the job object that ties the app's lifetime to the supervisor.
    LifetimeJob,
    /// Creating the pseudoconsole the app runs on.
    Pseudoconsole,
    /// Starting the app itself.
    App,
    /// Opening the pipe clients attach on.
    Listener,
    /// Learning the supervisor's own process identity.
    Identity,
    /// Reserving a session id.
    SessionId,
    /// Publishing the session record.
    PublishRecord,
}

impl StartupStep {
    /// What the supervisor was doing, for a sentence that says it failed.
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::LifetimeJob => "creating the session's lifetime job",
            Self::Pseudoconsole => "creating the session's console",
            Self::App => "starting the app",
            Self::Listener => "opening the session pipe",
            Self::Identity => "reading the supervisor's process identity",
            Self::SessionId => "reserving a session id",
            Self::PublishRecord => "publishing the session record",
        }
    }
}

// Startup-step bytes are stable assigned integers, like the kind bytes below.
const STEP_LIFETIME_JOB: u8 = 1;
const STEP_PSEUDOCONSOLE: u8 = 2;
const STEP_APP: u8 = 3;
const STEP_LISTENER: u8 = 4;
const STEP_IDENTITY: u8 = 5;
const STEP_SESSION_ID: u8 = 6;
const STEP_PUBLISH_RECORD: u8 = 7;

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
    // The frame is built in its final allocation: the length is written last,
    // over the room reserved for it here, so the body is copied once.
    let mut frame = vec![0_u8; size_of::<u32>()];
    let payload = &mut frame;
    match message {
        Message::Attach { size } => {
            payload.push(KIND_ATTACH);
            payload.extend_from_slice(&size.cols.get().to_le_bytes());
            payload.extend_from_slice(&size.rows.get().to_le_bytes());
        }
        Message::Input(data) => {
            payload.push(KIND_INPUT);
            payload.extend_from_slice(data);
        }
        Message::Resize { size } => {
            payload.push(KIND_RESIZE);
            payload.extend_from_slice(&size.cols.get().to_le_bytes());
            payload.extend_from_slice(&size.rows.get().to_le_bytes());
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
            pipe_name,
        } => {
            payload.push(KIND_STARTUP_OK);
            payload.extend_from_slice(&session_id.get().to_le_bytes());
            payload.push(match *launcher_tie {
                LauncherTie::NoneDetected => LAUNCHER_TIE_NONE_DETECTED,
                LauncherTie::Confirmed => LAUNCHER_TIE_CONFIRMED,
                LauncherTie::Unknown => LAUNCHER_TIE_UNKNOWN,
            });
            payload.extend_from_slice(pipe_name.as_bytes());
        }
        Message::StartupErr { step } => {
            payload.push(KIND_STARTUP_ERR);
            payload.push(match *step {
                StartupStep::LifetimeJob => STEP_LIFETIME_JOB,
                StartupStep::Pseudoconsole => STEP_PSEUDOCONSOLE,
                StartupStep::App => STEP_APP,
                StartupStep::Listener => STEP_LISTENER,
                StartupStep::Identity => STEP_IDENTITY,
                StartupStep::SessionId => STEP_SESSION_ID,
                StartupStep::PublishRecord => STEP_PUBLISH_RECORD,
            });
        }
        Message::StartupCommit => payload.push(KIND_STARTUP_COMMIT),
    }

    let len = u32::try_from(
        frame
            .len()
            .checked_sub(size_of::<u32>())
            .expect("the length prefix was reserved above"),
    )
    .expect("frame payload fits in u32");
    let (prefix, _) = frame
        .split_at_mut_checked(size_of::<u32>())
        .expect("the length prefix was reserved above");
    prefix.copy_from_slice(&len.to_le_bytes());
    frame
}

/// Failure while decoding a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeError {
    /// Bytes do not form a complete well-typed message.
    Invalid,
}

/// Decodes one frame whose kind byte the reader has already taken.
///
/// The byte-bearing messages take the body allocation as it is rather than
/// copying out of it, which is what keeps the relay from copying every
/// keystroke and every screenful of output a second time.
pub(crate) fn decode_body(kind: u8, body: Vec<u8>) -> Result<Message, DecodeError> {
    match kind {
        KIND_INPUT => Ok(Message::Input(body)),
        KIND_OUTPUT => Ok(Message::Output(body)),
        _ => decode_rest(kind, &body),
    }
}

fn decode_rest(kind: u8, rest: &[u8]) -> Result<Message, DecodeError> {
    match kind {
        KIND_ATTACH => decode_size(rest).map(|size| Message::Attach { size }),
        KIND_INPUT => Ok(Message::Input(rest.to_vec())),
        KIND_RESIZE => decode_size(rest).map(|size| Message::Resize { size }),
        KIND_ATTACHED => decode_session_id(rest).map(|session_id| Message::Attached { session_id }),
        KIND_OUTPUT => Ok(Message::Output(rest.to_vec())),
        KIND_DISPLACED if rest.is_empty() => Ok(Message::Displaced),
        KIND_APP_EXITED => decode_i32(rest).map(|status| Message::AppExited { status }),
        KIND_STARTUP_OK => {
            let Some((id_bytes, rest)) = rest.split_first_chunk::<{ size_of::<u32>() }>() else {
                return Err(DecodeError::Invalid);
            };
            let Some((launcher_tie, pipe_name)) = rest.split_first() else {
                return Err(DecodeError::Invalid);
            };
            let launcher_tie = match *launcher_tie {
                LAUNCHER_TIE_NONE_DETECTED => LauncherTie::NoneDetected,
                LAUNCHER_TIE_CONFIRMED => LauncherTie::Confirmed,
                LAUNCHER_TIE_UNKNOWN => LauncherTie::Unknown,
                _ => return Err(DecodeError::Invalid),
            };
            let Ok(pipe_name) = str::from_utf8(pipe_name) else {
                return Err(DecodeError::Invalid);
            };
            decode_session_id(id_bytes).map(|session_id| Message::StartupOk {
                session_id,
                launcher_tie,
                pipe_name: pipe_name.to_string(),
            })
        }
        KIND_STARTUP_ERR => {
            let [step] = rest else {
                return Err(DecodeError::Invalid);
            };
            let step = match *step {
                STEP_LIFETIME_JOB => StartupStep::LifetimeJob,
                STEP_PSEUDOCONSOLE => StartupStep::Pseudoconsole,
                STEP_APP => StartupStep::App,
                STEP_LISTENER => StartupStep::Listener,
                STEP_IDENTITY => StartupStep::Identity,
                STEP_SESSION_ID => StartupStep::SessionId,
                STEP_PUBLISH_RECORD => StartupStep::PublishRecord,
                _ => return Err(DecodeError::Invalid),
            };
            Ok(Message::StartupErr { step })
        }
        KIND_STARTUP_COMMIT if rest.is_empty() => Ok(Message::StartupCommit),
        _ => Err(DecodeError::Invalid),
    }
}

/// Returns whether a declared payload length is within the sanity cap.
#[must_use]
pub(crate) fn payload_len_ok(len: u32) -> bool {
    len > 0 && len <= MAX_FRAME_LEN
}

/// Decodes a console size, refusing one no console could have.
///
/// The invariant is established here rather than repaired further down, so a
/// peer that sends a zero dimension is told its frame is invalid instead of
/// silently getting a different size than it asked for.
fn decode_size(rest: &[u8]) -> Result<WindowSize, DecodeError> {
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
    WindowSize::new(cols, rows).ok_or(DecodeError::Invalid)
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

    /// Decodes a whole payload, kind byte and all.
    ///
    /// The transport reads the kind separately so a body allocation can become
    /// the message; a test is describing a frame, so it writes one.
    fn decode_payload(payload: &[u8]) -> Result<Message, DecodeError> {
        let Some((kind, rest)) = payload.split_first() else {
            return Err(DecodeError::Invalid);
        };
        decode_body(*kind, rest.to_vec())
    }

    #[test]
    fn round_trips_each_kind() {
        let id = SessionId::MIN;
        let messages = [
            Message::Attach {
                size: WindowSize::new(80, 24).expect("a fixture size is not empty"),
            },
            Message::Input(b"hi".to_vec()),
            Message::Resize {
                size: WindowSize::new(120, 30).expect("a fixture size is not empty"),
            },
            Message::Attached { session_id: id },
            Message::Output(b"out".to_vec()),
            Message::Displaced,
            Message::AppExited { status: 7 },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::NoneDetected,
                pipe_name: "\\\\.\\pipe\\dure-abc".to_string(),
            },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::Confirmed,
                pipe_name: "\\\\.\\pipe\\dure-abc".to_string(),
            },
            Message::StartupOk {
                session_id: id,
                launcher_tie: LauncherTie::Unknown,
                pipe_name: "\\\\.\\pipe\\dure-abc".to_string(),
            },
            Message::StartupErr {
                step: StartupStep::App,
            },
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
            decode_payload(&[KIND_STARTUP_COMMIT, 1]).unwrap_err(),
            DecodeError::Invalid
        );
    }

    #[test]
    fn a_startup_failure_names_a_step_this_build_assigned() {
        assert_eq!(
            decode_payload(&[KIND_STARTUP_ERR]).unwrap_err(),
            DecodeError::Invalid
        );
        assert_eq!(
            decode_payload(&[KIND_STARTUP_ERR, STEP_APP, 0]).unwrap_err(),
            DecodeError::Invalid
        );
        // A byte no step is assigned to; a later build may assign it.
        assert_eq!(
            decode_payload(&[KIND_STARTUP_ERR, 0]).unwrap_err(),
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
    fn a_size_no_console_could_have_is_refused() {
        // The invariant is established here so nothing below has to decide what
        // a zero means: a peer that sends one is told its frame is invalid
        // rather than quietly getting a different size than it asked for.
        for (cols, rows) in [(0_u16, 24_u16), (80, 0), (0, 0)] {
            let mut attach = vec![KIND_ATTACH];
            attach.extend_from_slice(&cols.to_le_bytes());
            attach.extend_from_slice(&rows.to_le_bytes());
            assert_eq!(
                decode_payload(&attach).unwrap_err(),
                DecodeError::Invalid,
                "{cols}x{rows} is not a console"
            );
        }
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
