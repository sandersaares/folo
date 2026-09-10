//! Verifies candidate source snapshots before release scripts create missing tags.
//!
//! This nonpublished controller utility reads a separate, caller-owned worktree;
//! GitHub operations and publication remain the responsibility of release orchestration.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::env::args_os;
use std::process::ExitCode;

use crate::cli::Cli;
use crate::verify::verify;

fn main() -> ExitCode {
    match Cli::parse(args_os().skip(1)).and_then(|cli| verify(&cli)) {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

mod cli;
mod command;
mod repository;
mod verify;
