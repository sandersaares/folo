//! Writing to the user's streams without panicking.
//!
//! The `print!` family panics when the stream behind it fails, which turns a
//! consumer that closed a pipe into an unwind through a command boundary. The
//! two kinds of output `dure` produces want different answers to that, so both
//! are stated here:
//!
//! * **Result output** — the session table, the resume prompt — is what the
//!   command was asked for, so a stream that cannot take it fails the command.
//! * **Diagnostics** — verbose notes, warnings, the session banner, the final
//!   error message — explain what happened. A stream that cannot take one is
//!   not worth failing over, least of all after a session has been committed,
//!   so they are best effort.

use std::fmt::Arguments;
use std::io::{self, Write as _};

/// Writes result output the command's success depends on.
///
/// # Errors
///
/// Returns the stream failure so the caller can decide what its command should
/// report.
pub(crate) fn print_line(message: Arguments<'_>) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{message}")?;
    stdout.flush()
}

/// Writes a diagnostic, giving up quietly if the stream will not take it.
pub(crate) fn note_line(message: Arguments<'_>) {
    let mut stderr = io::stderr().lock();
    _ = writeln!(stderr, "{message}");
    _ = stderr.flush();
}

/// Writes a prompt, without the newline the answer will follow.
///
/// # Errors
///
/// Returns the stream failure: a prompt nobody can see is not worth blocking a
/// read on.
// Writes to the process's own stderr, which no in-process test can observe, so
// a mutation that writes nothing is invisible here. The prompt's wording and
// the decision to ask are covered where they are made.
// Ref: docs/testing.md, "Mutation testing".
#[cfg_attr(test, mutants::skip)]
pub(crate) fn print_prompt(message: Arguments<'_>) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "{message}")?;
    stderr.flush()
}
