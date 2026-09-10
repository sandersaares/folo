//! Pure JSON controller for hosted scheduled-run evidence intake.
//!
//! The reviewed reporter invokes this utility with API observations on stdin. Only JSON is
//! emitted; GitHub calls, candidate-artifact validation and publication remain with the caller.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;
use std::process::ExitCode;

use scheduled_run_record::run;

fn main() -> ExitCode {
    match run(io::stdin().lock(), io::stdout().lock()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
