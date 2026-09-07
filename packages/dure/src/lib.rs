//! Detachable Windows console sessions that outlive the terminal.
//!
//! `dure` starts an interactive console process under a per-app supervisor, so the process keeps
//! running after the terminal that launched it goes away — a closed window, a dropped SSH
//! connection, or a killed foreground process. A later terminal attaches to the same process.
//!
//! ```text
//! dure run -- <command> [args...]   start a new session here and attach to it
//! dure resume [<id>]                attach to a live session, displacing any older client
//! dure list                         print live sessions
//! dure kill <id>                    abruptly terminate a session
//! ```
//!
//! This package ships a command-line tool. The Rust items it exports exist only so that the
//! binary in this same package and the package's own integration tests can reach the crate; they
//! are an implementation detail carrying no stability contract, which is why they are hidden from
//! the generated documentation.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
// The exported surface below is an implementation detail, so it is hidden from the rendered
// documentation. Hiding it only under `docsrs` is deliberate: the external-types check reads
// rustdoc JSON without that cfg, and an unconditional hide would take this surface out of the
// check's view instead of proving it exposes nothing. Ref: docs/external-types.md.
#![cfg_attr(docsrs, doc(hidden))]
// `dure` supervises Windows consoles and has no meaning on other platforms, so
// the whole crate is gated here rather than each module carrying its own
// platform stub (docs/implementation.md, "Platform gate").
#![cfg(windows)]

mod app_command;
mod attach;
mod cli;
mod commands;
mod constants;
mod detect;
mod dispatch;
mod durability;
mod errors;
mod gc;
mod invocation;
mod list_fmt;
mod outbox;
mod output;
mod pal;
mod path_display;
mod protocol;
mod session_id;
mod session_record;
mod supervisor;
mod trace;
mod wall_clock;

// The binary and the integration tests are the only intended consumers, so the surface is
// exported to reach them but not advertised as an API anyone may build on.
pub use app_command::AppCommand;
pub use cli::{Cli, EarlyExit};
pub use dispatch::run;
pub use invocation::{Command, Invocation, Outcome};
pub use session_id::SessionId;

// Helpers that exist only so integration tests can drive the Windows PAL, so
// they are test infrastructure rather than product code. Compiled in the crate's
// own test builds too, so the job topologies they model cannot stop compiling
// without a plain `cargo test` noticing (docs/feature-flags.md).
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(any(test, feature = "private-test-util"))]
pub mod test_support;

pub(crate) use errors::*;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;

    // The exported surface is what the binary hands across `run`, so it is pinned here rather
    // than in each defining module: this is the one place that says what the surface is.
    // Ref: docs/unwind-safety.md.
    assert_impl_all!(Cli: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(EarlyExit: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(SessionId: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(AppCommand: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(Command: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(Invocation: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(Outcome: UnwindSafe, RefUnwindSafe);

    // The test harness owns a thread it must join, so it holds a `JoinHandle`
    // and is deliberately neither unwind-safe nor ref-unwind-safe: a handle to
    // a thread that may have been left mid-work is exactly what those contracts
    // exist to refuse. It never crosses a `catch_unwind` boundary.
    // Ref: docs/unwind-safety.md.
    assert_not_impl_any!(test_support::ConsoleProcess: UnwindSafe, RefUnwindSafe);
}
