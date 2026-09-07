//! Binary entry point for the cargo-release-plan tool.

use std::env::args_os;
use std::process::ExitCode;

use cargo_release_plan::{Cli, RunOutcome, run};
#[cfg(not(miri))]
use mimalloc::MiMalloc;

// Same allocator as the other workspace Cargo subcommands (`cargo-detect-package`,
// `cargo-freeze-deps`). Miri cannot call mimalloc's FFI, so under Miri the
// default allocator stands in.
#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> ExitCode {
    let cli = match Cli::from_args_os(args_os()) {
        Ok(cli) => cli,
        Err(early_exit) => {
            // `status` is `Ok` for a `--help`/usage request (print to stdout, exit
            // success) and `Err` for a parse error (print to stderr, exit failure).
            return match early_exit.status {
                Ok(()) => {
                    println!("{}", early_exit.output);
                    ExitCode::SUCCESS
                }
                Err(()) => {
                    eprintln!("{}", early_exit.output);
                    ExitCode::FAILURE
                }
            };
        }
    };

    match run(&cli.into_input()) {
        Ok(outcome) => match outcome {
            RunOutcome::Report { message }
            | RunOutcome::Expand { message }
            | RunOutcome::Apply { message } => {
                if !message.is_empty() {
                    println!("{message}");
                }
                ExitCode::SUCCESS
            }
            RunOutcome::Check {
                passed,
                message,
                warnings,
            } => {
                if !message.is_empty() {
                    if passed {
                        println!("{message}");
                    } else {
                        eprintln!("{message}");
                    }
                }
                if !warnings.is_empty() {
                    eprint!("{warnings}");
                }
                if passed {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
        },
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
