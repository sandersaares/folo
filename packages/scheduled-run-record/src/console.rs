use std::io::{Read, Write};

use ohno::AppError;

use crate::execute;

/// Receives one request and writes its complete JSON response.
pub fn run(mut input: impl Read, mut output: impl Write) -> Result<(), AppError> {
    let mut text = String::new();
    input
        .read_to_string(&mut text)
        .map_err(ReadInputError::caused_by)?;
    // Rejected input must not emit a partial success response.
    let response = execute(&text)?;
    output
        .write_all(response.as_bytes())
        .map_err(WriteOutputError::caused_by)?;
    Ok(())
}

/// Identifies failure to receive a complete controller request.
#[ohno::error]
#[display("cannot read run-record input")]
pub(crate) struct ReadInputError;

/// Identifies failure to return a complete controller response.
#[ohno::error]
#[display("cannot write run-record output")]
pub(crate) struct WriteOutputError;
