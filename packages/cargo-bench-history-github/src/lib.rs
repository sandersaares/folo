#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(hidden))]

//! Unsupported GitHub automation companion for `cargo-bench-history`.
//!
//! The package is published only so the reusable action can install a tested
//! binary. Its library API and command line may change without a semver major
//! release.

mod cli;
mod errors;
mod github;
mod marker;
mod message;
mod model;
mod operations;

pub use cli::Cli;
pub use operations::run;
