#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Classifies publishable packages against version anchors and applies plans.

pub use check::CheckFormat;
pub use cli::{Cli, EarlyExit};
pub(crate) use errors::*;
pub use run::{RunInput, RunOutcome, run};
pub(crate) use text::{quote_path, short_commit};

/// Internal surface used only by this package's benchmarks.
#[cfg(any(test, feature = "private-test-util"))]
#[doc(hidden)]
pub mod __private {
    pub use crate::diff::benchmark_patch_rendering;
    pub use crate::lockfile::benchmark_lockfile_closures;
}

mod anchor;
mod apply;
mod check;
mod classify;
mod cli;
mod command;
mod diff;
mod errors;
mod expand;
mod git;
mod groups;
mod inherited;
mod lockfile;
mod manifest;
mod metadata;
mod packaging;
mod plan;
mod report;
mod run;
mod text;
mod verbose;
