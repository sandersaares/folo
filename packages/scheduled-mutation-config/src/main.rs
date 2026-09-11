//! Decodes cargo-mutants baseline configuration from stdin into JSON on stdout.
//!
//! Scheduled checker processes invoke this nonpublished utility to run unmutated baselines
//! for empty mutation shards with the same options as cargo-mutants.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::env::args_os;
use std::io::{self, Read, Write};
use std::process::ExitCode;

use ohno::AppError;

use crate::mutation_config::decode;

fn main() -> ExitCode {
    if args_os().len() != 1 {
        eprintln!("expected no arguments and mutation configuration on stdin");
        return ExitCode::FAILURE;
    }
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
    // Publish no success-shaped output until the whole document has passed validation.
    let json = decode(&text)?;
    output
        .write_all(json.as_bytes())
        .map_err(WriteOutputError::caused_by)?;
    Ok(())
}

/// Identifies a failure to receive the complete UTF-8 input document.
#[ohno::error]
#[display("cannot read mutation utility input from stdin")]
struct ReadInputError;

/// Identifies a failure to deliver the validated result to the caller.
#[ohno::error]
#[display("cannot write mutation utility result to stdout")]
struct WriteOutputError;

mod mutation_config;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn writes_only_validated_json() {
        let mut output = Vec::new();
        run("test_tool = 'nextest'".as_bytes(), &mut output).unwrap();
        let actual: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(actual.get("test_tool").unwrap(), "nextest");
    }

    #[test]
    fn failures_do_not_write_success_output() {
        for text in ["features = [", "all_features = 1", "test_tool = 'unknown'"] {
            let mut output = Vec::new();
            _ = run(text.as_bytes(), &mut output).unwrap_err();
            assert!(output.is_empty());
        }
    }

    #[test]
    fn invalid_utf8_preserves_read_error() {
        let mut output = Vec::new();
        let error = run([0xff].as_slice(), &mut output).unwrap_err();
        _ = error.find_source::<ReadInputError>().unwrap();
        _ = error.find_source::<io::Error>().unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn output_failure_preserves_write_error() {
        let error = run("".as_bytes(), [].as_mut_slice()).unwrap_err();
        _ = error.find_source::<WriteOutputError>().unwrap();
        _ = error.find_source::<io::Error>().unwrap();
    }
}
