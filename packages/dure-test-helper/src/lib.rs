//! Locator for the `dure-test-helper` binary.
//!
//! The helper binary is the controllable child process the `dure` Windows
//! integration tests drive inside a pseudoconsole. It lives in its own package
//! so `dure` ships exactly one binary; that also means Cargo sets no
//! `CARGO_BIN_EXE_*` variable for it in `dure`'s tests, so this crate resolves
//! the path instead (`dure/docs/implementation.md`, "Integration tests").

// The helper only has meaning for the Windows integration tests, so the whole
// crate is gated here rather than carrying per-item platform stubs, matching
// `dure` itself (`dure/docs/implementation.md`, "Platform gate").
#![cfg(windows)]

mod locate;
mod non_ascii;
mod terminal_input;

pub use locate::binary_path;
pub use non_ascii::SAMPLE_NON_ASCII_TEXT;
pub use terminal_input::*;
