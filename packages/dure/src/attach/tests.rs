//! Attach and relay scenarios driven through mock PAL implementations.

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use std::vec;

use super::*;
use crate::durability::LauncherTie;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{ConnId, ListenerId};
use crate::pal::local_console::{LocalConsoleFacade, MockLocalConsole};
use crate::pal::pseudoconsole::WindowSize;
use crate::pal::transport::MemoryTransport;
use crate::protocol::Message;

const SAMPLE_SIZE: WindowSize = WindowSize::new(80, 24).expect("a fixture size is not empty");

/// Console input a test hands to the relay, and the cancellation that ends it.
///
/// A real console read blocks until the user types or the read is cancelled,
/// and the relay relies on cancellation to retire its reader before handing the
/// console back. Modelling both is what lets these tests join every thread they
/// start instead of leaving one parked for the life of the process.
#[derive(Debug, Default)]
struct ConsoleScript {
    state: Mutex<ScriptState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct ScriptState {
    input: VecDeque<Result<ConsoleInput, PalErrorKind>>,
    cancelled: bool,
}

impl ConsoleScript {
    fn new(input: Vec<Result<ConsoleInput, PalErrorKind>>) -> Self {
        Self {
            state: Mutex::new(ScriptState {
                input: input.into(),
                cancelled: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn read(&self) -> Result<ConsoleInput, PalError> {
        let mut state = self.state.lock().unwrap();
        loop {
            if state.cancelled {
                return Err(PalError::new(PalErrorKind::Disconnected));
            }
            if let Some(next) = state.input.pop_front() {
                return next.map_err(PalError::new);
            }
            state = self.changed.wait(state).unwrap();
        }
    }

    fn cancel(&self) {
        self.state.lock().unwrap().cancelled = true;
        self.changed.notify_all();
    }
}

/// Assembles the `LocalConsole` an attach test needs: the happy path by
/// default, with individual operations overridden to fail and with console
/// input supplied as a script.
struct TestConsole {
    has_console: bool,
    begin_raw_relay: Result<(), PalErrorKind>,
    end_raw_relay: Result<(), PalErrorKind>,
    window_size: Result<(), PalErrorKind>,
    write_output: Result<(), PalErrorKind>,
    input: Vec<Result<ConsoleInput, PalErrorKind>>,
    hand_backs: Arc<AtomicUsize>,
}

impl TestConsole {
    fn new() -> Self {
        Self {
            has_console: true,
            begin_raw_relay: Ok(()),
            end_raw_relay: Ok(()),
            window_size: Ok(()),
            write_output: Ok(()),
            input: Vec::new(),
            hand_backs: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn build(self) -> LocalConsoleFacade {
        let Self {
            has_console,
            begin_raw_relay,
            end_raw_relay,
            window_size,
            write_output,
            input,
            hand_backs,
        } = self;

        let mut console = MockLocalConsole::new();
        console.expect_has_console().return_const(has_console);
        console.expect_begin_raw_relay().returning(move || {
            begin_raw_relay
                .map(|()| RelayLeaseId::for_test(1))
                .map_err(PalError::new)
        });
        console.expect_end_raw_relay().returning(move |_lease| {
            hand_backs.fetch_add(1, Ordering::SeqCst);
            end_raw_relay.map_err(PalError::new)
        });
        console
            .expect_window_size()
            .returning(move || window_size.map(|()| SAMPLE_SIZE).map_err(PalError::new));
        console
            .expect_write_output()
            .returning(move |_| write_output.map_err(PalError::new));

        let script = Arc::new(ConsoleScript::new(input));
        console.expect_read_input().returning({
            let script = Arc::clone(&script);
            move || script.read()
        });
        console.expect_cancel_input().returning({
            let script = Arc::clone(&script);
            move || {
                script.cancel();
                Ok(())
            }
        });

        LocalConsoleFacade::from_mock(console)
    }
}

/// Transport whose every operation fails, with a configurable `connect` kind.
///
/// Exercises the attach paths that precede a working connection.
#[derive(Clone, Debug)]
struct ConnectFails(PalErrorKind);

impl Transport for ConnectFails {
    fn listen(&self, _name: &str) -> Result<ListenerId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn accept(&self, _listener: ListenerId) -> Result<ConnId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn accept_timeout(
        &self,
        _listener: ListenerId,
        _timeout: Duration,
    ) -> Result<ConnId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn connect(&self, _name: &str, _timeout: Duration) -> Result<ConnId, PalError> {
        Err(PalError::new(self.0))
    }

    fn send(&self, _conn: ConnId, _message: &Message) -> Result<(), PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn recv(&self, _conn: ConnId) -> Result<Message, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn recv_timeout(&self, _conn: ConnId, _timeout: Duration) -> Result<Message, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn disconnect(&self, _conn: ConnId) {}

    fn close_listener(&self, _listener: ListenerId) {}

    fn pipe_name(&self, nonce: &str) -> String {
        nonce.to_string()
    }
}

/// Transport that connects but cannot send, so the handshake fails on the
/// `Attach` message rather than on the connection itself.
#[derive(Clone, Debug)]
struct SendFails;

impl Transport for SendFails {
    fn listen(&self, _name: &str) -> Result<ListenerId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn accept(&self, _listener: ListenerId) -> Result<ConnId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn accept_timeout(
        &self,
        _listener: ListenerId,
        _timeout: Duration,
    ) -> Result<ConnId, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn connect(&self, _name: &str, _timeout: Duration) -> Result<ConnId, PalError> {
        Ok(ConnId::for_test(1))
    }

    fn send(&self, _conn: ConnId, _message: &Message) -> Result<(), PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn recv(&self, _conn: ConnId) -> Result<Message, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn recv_timeout(&self, _conn: ConnId, _timeout: Duration) -> Result<Message, PalError> {
        Err(PalError::new(PalErrorKind::Other))
    }

    fn disconnect(&self, _conn: ConnId) {}

    fn close_listener(&self, _listener: ListenerId) {}

    fn pipe_name(&self, nonce: &str) -> String {
        nonce.to_string()
    }
}

/// Runs `attach` against a supervisor stand-in that completes the handshake
/// and then hands the live connection to `serve`.
///
/// The stand-in is joined before the result is returned, so a panic inside it
/// fails this test rather than some later one.
fn attach_to_scripted_supervisor<F>(console: TestConsole, serve: F) -> Result<Outcome, AppError>
where
    F: FnOnce(&MemoryTransport, ConnId) + Send + 'static,
{
    let transport = MemoryTransport::new();
    let listener = transport.listen("pipe").unwrap();
    let id = SessionId::MIN;
    let supervisor = thread::spawn({
        let transport = transport.clone();
        move || {
            let conn = transport.accept(listener).unwrap();
            _ = transport.recv(conn);
            _ = transport.send(conn, &Message::Attached { session_id: id });
            serve(&transport, conn);
        }
    });
    let outcome = attach(&transport, &console.build(), "pipe", id);
    supervisor.join().unwrap();
    outcome
}

#[test]
fn without_a_console_attach_is_refused() {
    let error = attach(
        &ConnectFails(PalErrorKind::Other),
        &TestConsole {
            has_console: false,
            ..TestConsole::new()
        }
        .build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<NoConsoleError>().is_some());
}

#[test]
fn a_refused_takeover_is_pal_failure() {
    let console = TestConsole {
        begin_raw_relay: Err(PalErrorKind::Other),
        hand_backs: Arc::new(AtomicUsize::new(0)),
        ..TestConsole::new()
    };
    let hand_backs = Arc::clone(&console.hand_backs);
    let error = attach(
        &ConnectFails(PalErrorKind::Other),
        &console.build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<PalFailedError>().is_some());
    // Nothing was taken, so nothing is handed back.
    assert_eq!(hand_backs.load(Ordering::SeqCst), 0);
}

#[test]
fn window_size_failure_is_pal_failure() {
    let error = attach(
        &ConnectFails(PalErrorKind::Other),
        &TestConsole {
            window_size: Err(PalErrorKind::Other),
            ..TestConsole::new()
        }
        .build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<PalFailedError>().is_some());
}

#[test]
fn handshake_send_failure_is_attach_failure() {
    let error = attach(
        &SendFails,
        &TestConsole::new().build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<AttachFailedError>().is_some());
}

#[test]
fn attached_id_mismatch_is_attach_failure() {
    let transport = MemoryTransport::new();
    let listener = transport.listen("pipe").unwrap();
    // Built here rather than inside the thread: a panic in a spawned
    // thread would leave `attach` blocked in `recv` forever.
    let other = SessionId::from_u32(2).unwrap();
    let supervisor = thread::spawn({
        let transport = transport.clone();
        move || {
            let conn = transport.accept(listener).unwrap();
            _ = transport.recv(conn);
            _ = transport.send(conn, &Message::Attached { session_id: other });
            transport.disconnect(conn);
        }
    });
    let error = attach(
        &transport,
        &TestConsole::new().build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    supervisor.join().unwrap();
    assert!(error.find_source::<AttachFailedError>().is_some());
}

#[test]
fn matching_attached_then_app_exit_is_success() {
    let outcome = attach_to_scripted_supervisor(TestConsole::new(), |transport, conn| {
        _ = transport.send(conn, &Message::AppExited { status: 3 });
    })
    .unwrap();
    assert!(matches!(outcome, Outcome::AppExit(3)));
}

#[test]
fn displaced_handshake_is_displaced() {
    let transport = MemoryTransport::new();
    let listener = transport.listen("pipe").unwrap();
    let supervisor = thread::spawn({
        let transport = transport.clone();
        move || {
            let conn = transport.accept(listener).unwrap();
            _ = transport.recv(conn);
            _ = transport.send(conn, &Message::Displaced);
            transport.disconnect(conn);
        }
    });
    let error = attach(
        &transport,
        &TestConsole::new().build(),
        "pipe",
        SessionId::MIN,
    )
    .unwrap_err();
    supervisor.join().unwrap();
    assert!(error.find_source::<DisplacedError>().is_some());
}

#[test]
fn connect_timeout_is_resume_timeout() {
    let error = attach(
        &ConnectFails(PalErrorKind::Timeout),
        &TestConsole::new().build(),
        "missing",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<ResumeTimeoutError>().is_some());
}

#[test]
fn connect_other_is_attach_failure() {
    let error = attach(
        &ConnectFails(PalErrorKind::Other),
        &TestConsole::new().build(),
        "missing",
        SessionId::MIN,
    )
    .unwrap_err();
    assert!(error.find_source::<AttachFailedError>().is_some());
}

#[test]
fn the_console_is_handed_back_when_attach_fails() {
    let hand_backs = Arc::new(AtomicUsize::new(0));
    attach(
        &ConnectFails(PalErrorKind::Other),
        &TestConsole {
            hand_backs: Arc::clone(&hand_backs),
            ..TestConsole::new()
        }
        .build(),
        "missing",
        SessionId::MIN,
    )
    .unwrap_err();
    assert_eq!(hand_backs.load(Ordering::SeqCst), 1);
}

#[test]
fn the_console_is_handed_back_once_after_a_completed_relay() {
    let console = TestConsole::new();
    let hand_backs = Arc::clone(&console.hand_backs);
    attach_to_scripted_supervisor(console, |transport, conn| {
        _ = transport.send(conn, &Message::AppExited { status: 0 });
    })
    .unwrap();
    assert_eq!(hand_backs.load(Ordering::SeqCst), 1);
}

#[test]
fn a_console_that_cannot_be_handed_back_still_forwards_the_app_status() {
    let console = TestConsole {
        end_raw_relay: Err(PalErrorKind::Other),
        ..TestConsole::new()
    };
    let outcome = attach_to_scripted_supervisor(console, |transport, conn| {
        _ = transport.send(conn, &Message::AppExited { status: 7 });
    })
    .unwrap();
    // The app ran and said what it did; that status is the command's result
    // whatever happened to the console afterwards.
    assert!(matches!(outcome, Outcome::AppExit(7)));
}

#[test]
fn a_console_that_cannot_be_handed_back_fails_a_relay_with_nothing_to_report() {
    let console = TestConsole {
        end_raw_relay: Err(PalErrorKind::Other),
        ..TestConsole::new()
    };
    let error = attach_to_scripted_supervisor(console, |transport, conn| {
        _ = transport.send(conn, &Message::Displaced);
    })
    .unwrap_err();
    // The displacement is the cause and stays the reported one.
    assert!(error.find_source::<DisplacedError>().is_some());
}

#[test]
fn the_console_can_be_taken_over_again_after_a_relay() {
    for status in [1, 2] {
        let outcome = attach_to_scripted_supervisor(TestConsole::new(), move |transport, conn| {
            _ = transport.send(conn, &Message::AppExited { status });
        })
        .unwrap();
        assert!(matches!(outcome, Outcome::AppExit(exited) if exited == status));
    }
}

#[test]
fn console_input_is_forwarded_to_the_supervisor() {
    let resize = WindowSize::new(10, 20).expect("a fixture size is not empty");
    let console = TestConsole {
        input: vec![
            Ok(ConsoleInput::Bytes(b"hi".to_vec())),
            Ok(ConsoleInput::Resize(resize)),
        ],
        ..TestConsole::new()
    };
    let outcome = attach_to_scripted_supervisor(console, move |transport, conn| {
        assert!(matches!(
            transport.recv(conn),
            Ok(Message::Input(bytes)) if bytes == b"hi"
        ));
        assert!(matches!(
            transport.recv(conn),
            Ok(Message::Resize { size }) if size == resize
        ));
        _ = transport.send(conn, &Message::AppExited { status: 0 });
    })
    .unwrap();
    assert!(matches!(outcome, Outcome::AppExit(0)));
}

#[test]
fn console_input_failure_makes_the_relay_fail() {
    let console = TestConsole {
        input: vec![Err(PalErrorKind::Other)],
        ..TestConsole::new()
    };
    // The input thread disconnects, so the output loop sees the peer close
    // without an `AppExited` and must not report success.
    let error = attach_to_scripted_supervisor(console, |_transport, _conn| {}).unwrap_err();
    assert!(error.find_source::<RelayFailedError>().is_some());
}

#[test]
fn output_write_failure_is_relay_failure() {
    let console = TestConsole {
        write_output: Err(PalErrorKind::Other),
        ..TestConsole::new()
    };
    let error = attach_to_scripted_supervisor(console, |transport, conn| {
        _ = transport.send(conn, &Message::Output(b"out".to_vec()));
    })
    .unwrap_err();
    assert!(error.find_source::<RelayFailedError>().is_some());
}

#[test]
fn a_supervisor_that_disconnects_without_saying_why_is_a_lost_session() {
    // `kill`, a crash, and a lost pipe all look like this, and none of them is
    // the app reporting that it finished. Ref: docs/design.md, "Lifetime".
    let error = attach_to_scripted_supervisor(TestConsole::new(), |transport, conn| {
        _ = transport.send(conn, &Message::Output(b"out".to_vec()));
        transport.disconnect(conn);
    })
    .unwrap_err();
    assert!(error.find_source::<SupervisorLostError>().is_some());
}

#[test]
fn displacement_during_relay_is_displaced() {
    let error = attach_to_scripted_supervisor(TestConsole::new(), |transport, conn| {
        _ = transport.send(conn, &Message::Displaced);
    })
    .unwrap_err();
    assert!(error.find_source::<DisplacedError>().is_some());
}

#[test]
fn unexpected_relay_message_is_relay_failure() {
    let error = attach_to_scripted_supervisor(TestConsole::new(), |transport, conn| {
        _ = transport.send(
            conn,
            &Message::StartupOk {
                session_id: SessionId::MIN,
                launcher_tie: LauncherTie::NoneDetected,
            },
        );
    })
    .unwrap_err();
    assert!(error.find_source::<RelayFailedError>().is_some());
}
