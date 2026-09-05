//! Client attach and console relay.
//!
//! Ref: docs/console.md, "Modes".

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use ohno::AppError;

use crate::constants::CONNECT_TIMEOUT;
use crate::output::note_line;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{ConnId, RelayLeaseId};
use crate::pal::local_console::{ConsoleInput, LocalConsole};
use crate::pal::transport::Transport;
use crate::protocol::Message;
use crate::{
    AttachFailedError, ConsoleRestoreError, DisplacedError, NoConsoleError, Outcome,
    PalFailedError, RelayFailedError, ResumeTimeoutError, SessionId, SupervisorLostError,
};

/// Connect to a live supervisor and funnel console I/O until the relay ends.
///
/// The caller reports the session id before calling this, because it already
/// knows it and a failure in here still leaves that session reachable through
/// `list`, `resume`, and `kill`.
pub(crate) fn attach<T, C>(
    transport: &T,
    console: &C,
    pipe_name: &str,
    session_id: SessionId,
) -> Result<Outcome, AppError>
where
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    if !console.has_console() {
        return Err(NoConsoleError::new().into());
    }
    let lease = ConsoleLease::take(console).map_err(PalFailedError::caused_by)?;
    // Read after taking the console over so the size is the one the app will be
    // rendered at, and sent with `Attach` so the supervisor can apply it as
    // part of taking the client slot.
    // Ref: docs/console.md, "Window size".
    let outcome = handshake_and_relay(transport, console, pipe_name, session_id);
    // The console is handed back explicitly on every ordinary return, so a
    // restoration failure is a fact the caller learns about rather than one the
    // guard swallows on the way out.
    let restored = lease.release();
    finish(outcome, restored)
}

/// Combines the relay's outcome with the console hand-back.
///
/// An app that ran is entitled to have its exit status forwarded, so a failed
/// hand-back is reported as a diagnostic beside that status rather than
/// replacing it; a command that has nothing else to report fails outright.
/// Ref: docs/design.md, "Console I/O".
fn finish(
    outcome: Result<Outcome, AppError>,
    restored: Result<(), AppError>,
) -> Result<Outcome, AppError> {
    match (outcome, restored) {
        (outcome, Ok(())) => outcome,
        (Ok(Outcome::AppExit(status)), Err(error)) => {
            note_line(format_args!("Warning: {error}"));
            Ok(Outcome::AppExit(status))
        }
        (Ok(Outcome::Success), Err(error)) => Err(error),
        // The relay already failed; that is the cause, and a console left raw
        // is the consequence of the same lost session.
        (Err(error), Err(_restore_error)) => Err(error),
    }
}

fn handshake_and_relay<T, C>(
    transport: &T,
    console: &C,
    pipe_name: &str,
    session_id: SessionId,
) -> Result<Outcome, AppError>
where
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    let size = console.window_size().map_err(PalFailedError::caused_by)?;

    let conn = match transport.connect(pipe_name, CONNECT_TIMEOUT) {
        Ok(conn) => conn,
        Err(error) if error.kind() == PalErrorKind::Timeout => {
            return Err(ResumeTimeoutError::for_id(session_id).into());
        }
        Err(_) => return Err(AttachFailedError::for_id(session_id).into()),
    };

    transport
        .send(conn, &Message::Attach { size })
        .map_err(|_error| AttachFailedError::for_id(session_id))?;

    match transport.recv(conn) {
        Ok(Message::Attached {
            session_id: attached_id,
        }) if attached_id == session_id => {}
        Ok(Message::Displaced) => {
            transport.disconnect(conn);
            return Err(DisplacedError::new().into());
        }
        _ => {
            transport.disconnect(conn);
            return Err(AttachFailedError::for_id(session_id).into());
        }
    }

    relay(transport, console, conn)
}

/// The local console, taken over for one relay and owed back.
///
/// [`ConsoleLease::release`] is the ordinary way out and reports whether the
/// console was handed back; `Drop` only covers an unwind, where there is
/// nobody left to tell.
struct ConsoleLease<'a, C: LocalConsole> {
    console: &'a C,
    lease: Option<RelayLeaseId>,
}

impl<'a, C: LocalConsole> ConsoleLease<'a, C> {
    fn take(console: &'a C) -> Result<Self, PalError> {
        let lease = console.begin_raw_relay()?;
        Ok(Self {
            console,
            lease: Some(lease),
        })
    }

    // The only thing this reports that `Drop` does not do anyway is a failed
    // hand-back, which reaches the user as a line on stderr. A mutation that
    // stops reporting it is therefore not observable in this process.
    // Ref: docs/testing.md, "Mutation testing".
    #[cfg_attr(test, mutants::skip)]
    fn release(mut self) -> Result<(), AppError> {
        self.lease.take().map_or(Ok(()), |lease| {
            self.console
                .end_raw_relay(lease)
                .map_err(|error| ConsoleRestoreError::caused_by(error).into())
        })
    }
}

impl<C: LocalConsole> Drop for ConsoleLease<'_, C> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            _ = self.console.end_raw_relay(lease);
        }
    }
}

fn relay<T, C>(transport: &T, console: &C, conn: ConnId) -> Result<Outcome, AppError>
where
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    let input_failed = Arc::new(AtomicBool::new(false));
    let reader = spawn_input_reader(transport, console, conn, &input_failed);

    let outcome = receive_until_relay_ends(transport, console, conn);
    // Read before the reader is cancelled: a reader that failed set this flag
    // and then disconnected, which is what ended the receive above, while a
    // reader cancelled from here reports the same disconnect for a reason that
    // is not a failure at all.
    let input_failed = input_failed.load(Ordering::SeqCst);

    // The reader owns the console until it stops, so the console is not handed
    // back while a blocked read could still take the caller's next keystroke.
    _ = console.cancel_input();
    _ = reader.join();

    match outcome {
        RelayEnd::AppExited(status) => Ok(Outcome::AppExit(status)),
        RelayEnd::Displaced => Err(DisplacedError::new().into()),
        RelayEnd::Failed => Err(RelayFailedError::new().into()),
        RelayEnd::SupervisorGone => {
            if input_failed {
                Err(RelayFailedError::new().into())
            } else {
                Err(SupervisorLostError::new().into())
            }
        }
    }
}

/// Forwards console input until the relay ends or the console stops reading.
fn spawn_input_reader<T, C>(
    transport: &T,
    console: &C,
    conn: ConnId,
    input_failed: &Arc<AtomicBool>,
) -> thread::JoinHandle<()>
where
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    thread::spawn({
        let transport = transport.clone();
        let console = console.clone();
        let input_failed = Arc::clone(input_failed);
        move || {
            loop {
                match console.read_input() {
                    Ok(ConsoleInput::Bytes(bytes)) => {
                        if transport.send(conn, &Message::Input(bytes)).is_err() {
                            break;
                        }
                    }
                    Ok(ConsoleInput::Resize(size)) => {
                        if transport.send(conn, &Message::Resize { size }).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        input_failed.store(true, Ordering::SeqCst);
                        transport.disconnect(conn);
                        break;
                    }
                }
            }
        }
    })
}

/// How an attached relay ended, before it is turned into a command outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RelayEnd {
    /// The supervisor reported the app's exit status.
    AppExited(i32),
    /// Another client took the session over.
    Displaced,
    /// The local console could not be driven any further.
    Failed,
    /// The connection closed without a terminal message.
    SupervisorGone,
}

// Blocking recv. A mutation that drops the disconnect this makes on the way out
// hangs the input reader's join, because watchdogs are disabled under
// cargo-mutants. The classification above it stays under mutation coverage.
#[cfg_attr(test, mutants::skip)]
fn receive_until_relay_ends<T, C>(transport: &T, console: &C, conn: ConnId) -> RelayEnd
where
    T: Transport + Clone + Send + Sync + 'static,
    C: LocalConsole + Clone + Send + Sync + 'static,
{
    let end = loop {
        match transport.recv(conn) {
            Ok(Message::Output(bytes)) => {
                if console.write_output(&bytes).is_err() {
                    break RelayEnd::Failed;
                }
            }
            Ok(Message::AppExited { status }) => break RelayEnd::AppExited(status),
            Ok(Message::Displaced) => break RelayEnd::Displaced,
            Err(error) if error.kind() == PalErrorKind::Disconnected => {
                break RelayEnd::SupervisorGone;
            }
            Ok(_) | Err(_) => break RelayEnd::Failed,
        }
    };
    transport.disconnect(conn);
    end
}
