//! Pure JSON controller for hosted scheduled-run evidence intake.
//!
//! The reviewed reporter invokes this utility with API observations on stdin. Only JSON is
//! emitted; GitHub calls, candidate-artifact validation and publication remain with the caller.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io::{self, Read, Write};
use std::process::ExitCode;

use ohno::AppError;

use crate::protocol::execute;

fn main() -> ExitCode {
    match run(io::stdin().lock(), io::stdout().lock()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(mut input: impl Read, mut output: impl Write) -> Result<(), AppError> {
    let mut text = String::new();
    input
        .read_to_string(&mut text)
        .map_err(ReadInputError::caused_by)?;
    // Build the entire response first so rejected input never produces success-shaped JSON.
    let response = execute(&text)?;
    output
        .write_all(response.as_bytes())
        .map_err(WriteOutputError::caused_by)?;
    Ok(())
}

/// Identifies failure to receive a complete controller request.
#[ohno::error]
#[display("cannot read run-record input")]
struct ReadInputError;

/// Identifies failure to return a complete controller response.
#[ohno::error]
#[display("cannot write run-record output")]
struct WriteOutputError;

mod canonical;
mod evidence;
mod gap_paths;
mod pages;
mod protocol;
mod record;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
