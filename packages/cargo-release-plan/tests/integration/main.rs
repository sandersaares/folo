//! End-to-end tests against hermetic Git fixtures.
//!
//! Each test drives [`cargo_release_plan::run`] directly, except for
//! [`cli_binary`], which drives the installed subcommand as a process. Git
//! configuration is pinned by [`fixture::Fixture`] so tests do not depend on
//! host or user settings. Integer literals assigned to unused locals in
//! generated Rust sources are arbitrary byte-change markers.
//!
//! The suite is split into one topic module per area of behavior over a shared
//! [`harness`]; this file is the crate root that ties the modules together.

mod apply;
mod baseline;
mod cli_binary;
mod expand;
mod fixture;
mod groups;
mod harness;
mod history;
mod lockfile;
mod nesting;
mod packaging;
mod report;
mod status;
mod version_groups;
