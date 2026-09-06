//! Subcommand implementations.
//!
//! Each module here is the thin wrapper for one subcommand. The long-lived
//! supervisor role itself lives in [`crate::supervisor`]; `supervise` is only
//! the hidden subcommand that enters it.

pub(crate) mod kill;
pub(crate) mod list;
pub(crate) mod resume;
pub(crate) mod run;
pub(crate) mod supervise;
