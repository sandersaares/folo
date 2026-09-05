//! Literal values that encode design and implementation decisions.

use std::num::NonZero;
use std::time::Duration;

/// Bound so `resume` fails instead of hanging when the supervisor is alive but
/// not accepting.
///
/// Local named-pipe connect is immediate when the supervisor is listening.
/// The duration is an arbitrary watchdog, chosen to sit well above local IPC
/// and well below a human "this is stuck" wait.
/// Ref: docs/design.md, "Attach, detach, steal"; docs/supervisor.md.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on supervisor initialization after it has connected to the client.
///
/// Starting an app may involve executable lookup, security scanning, and operating-system
/// process setup, so it needs human-scale headroom beyond local IPC. The wait remains bounded
/// so a supervisor stalled during initialization cannot hang `dure run`.
/// Ref: docs/implementation.md, "Process split".
pub(crate) const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on how long `terminate` waits for a killed process to become signaled.
///
/// `TerminateProcess` only initiates termination; the process object signals
/// once the kernel has finished tearing it down, which is immediate unless a
/// driver stalls. The duration is an arbitrary watchdog, chosen to sit well
/// above that teardown and well below a human "this is stuck" wait.
#[cfg_attr(
    not(windows),
    expect(dead_code, reason = "consumed by the Windows process PAL")
)]
pub(crate) const TERMINATE_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on one framed transport message.
///
/// Ordinary console bursts are kibibytes. This is an arbitrary sanity cap so a
/// corrupt length prefix cannot force a huge allocation.
pub(crate) const MAX_FRAME_LEN: u32 = 1024 * 1024;

/// Largest `Output` payload that fits in one frame.
///
/// A frame's length prefix counts the message kind byte as well as the data, so
/// the data has to stay one byte below the frame cap. It is a chunk size, and a
/// chunk size of zero would divide the payload into infinitely many pieces, so
/// a frame cap too small to carry any payload is a contradiction rather than a
/// value to carry forward.
/// Ref: docs/supervisor.md, "Opening output".
pub(crate) const MAX_OUTPUT_CHUNK_BYTES: NonZero<usize> =
    match NonZero::new((MAX_FRAME_LEN as usize).saturating_sub(1)) {
        Some(size) => size,
        None => panic!("the frame cap must leave room for payload"),
    };

/// Output bytes the supervisor will hold for a client before giving up on it.
///
/// A client this far behind is not draining its pipe. `dure` keeps no screen
/// buffer, so undelivered output has no later value and the session is better
/// served by dropping the connection than by growing without bound. The size is
/// arbitrary, chosen to sit well above any burst a responsive client causes and
/// well below a memory footprint worth worrying about.
/// Ref: docs/transport.md.
pub(crate) const MAX_CLIENT_BACKLOG_BYTES: usize = 4 * 1024 * 1024;

/// Subdirectory under the per-user `LocalAppData` known folder that holds
/// session records.
///
/// Ref: docs/session-store.md.
#[cfg_attr(
    not(windows),
    expect(
        dead_code,
        reason = "joined onto LocalAppData by the Windows store root"
    )
)]
pub(crate) const STORE_SUBDIR: &str = "dure";

/// Hidden subcommand that `dure run` uses to spawn the supervisor process.
///
/// Windows has no `exec`; the same binary implements both roles.
/// Ref: docs/implementation.md, "Process split".
pub(crate) const SUPERVISOR_COMMAND: &str = "__supervisor";

/// Columns used until the first client attach reports a real size.
///
/// VGA text-mode geometry, the historical Windows console default.
/// Ref: docs/design.md, "Terminal pass-through".
pub(crate) const DEFAULT_PTY_COLS: NonZero<u16> = NonZero::new(80).expect("80 is not zero");

/// Rows used until the first client attach reports a real size.
///
/// VGA text-mode geometry, the historical Windows console default.
/// Ref: docs/design.md, "Terminal pass-through".
pub(crate) const DEFAULT_PTY_ROWS: NonZero<u16> = NonZero::new(24).expect("24 is not zero");
