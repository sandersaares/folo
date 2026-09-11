//! Pure structural validation for analysis supplied by the Local App's selected AI.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io::{self, Read, Write};
use std::process::ExitCode;

use ohno::AppError;

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
        .map_err(ReadRequestError::caused_by)?;
    let response = protocol::execute(&text)?;
    output
        .write_all(response.as_bytes())
        .map_err(WriteResponseError::caused_by)?;
    Ok(())
}

/// Identifies an incomplete or invalid UTF-8 request stream.
#[ohno::error]
#[display("cannot read triage request")]
struct ReadRequestError;

/// Identifies failure to deliver the complete validated response.
#[ohno::error]
#[display("cannot write triage response")]
struct WriteResponseError;

mod analysis;
mod basis;
mod comparison;
mod lifecycle;
mod problem;
mod protocol;
mod scope;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
