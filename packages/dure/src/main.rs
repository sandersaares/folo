#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Binary entry point for `dure`.

#[cfg(windows)]
use std::env;
#[cfg(windows)]
use std::io::{self, Write};
#[cfg(windows)]
use std::process::{self, ExitCode};

#[cfg(windows)]
use dure::{Cli, Outcome, run};

// Install mimalloc as a scalable, general-purpose allocator process-wide: faster
// small allocations and no cross-thread allocator-lock contention (acute on the
// Windows process heap), a broad low-risk win applied uniformly across the
// workspace's binaries. Miri cannot call mimalloc's FFI, so under Miri the
// default allocator stands in.
#[cfg(all(windows, not(miri)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// `dure` drives Windows consoles and has no meaning elsewhere
/// (implementation.md, "Platform gate"), so the binary refuses to run rather
/// than reporting a success it did not deliver.
// Mutation testing runs on Windows, where this stub is not compiled and no test
// can observe a mutation of it.
#[cfg(not(windows))]
#[cfg_attr(test, mutants::skip)]
fn main() -> std::process::ExitCode {
    eprintln!("Error: dure runs only on Windows.");
    std::process::ExitCode::FAILURE
}

#[cfg(windows)]
#[cfg_attr(test, mutants::skip)]
fn main() -> ExitCode {
    let Some(env_args) = env::args_os()
        .map(|arg| arg.into_string().ok())
        .collect::<Option<Vec<String>>>()
    else {
        // The app is launched with the argv given here, so mangling it into
        // something the shell did not ask for is worse than refusing.
        write_line(&mut io::stderr(), "Error: every argument must be valid Unicode.");
        return ExitCode::FAILURE;
    };
    let str_args: Vec<&str> = env_args.iter().map(String::as_str).collect();
    let program_name = str_args.first().map_or("dure", |name| *name);

    let cli = match Cli::from_args(&[program_name], str_args.get(1..).unwrap_or(&[])) {
        Ok(cli) => cli,
        Err(early_exit) => {
            return match early_exit.status {
                Ok(()) => {
                    write_line(&mut io::stdout(), &early_exit.output);
                    ExitCode::SUCCESS
                }
                Err(()) => {
                    write_line(&mut io::stderr(), &early_exit.output);
                    ExitCode::FAILURE
                }
            };
        }
    };

    let invocation = match cli.into_invocation() {
        Ok(invocation) => invocation,
        Err(early_exit) => {
            write_line(&mut io::stderr(), &early_exit.output);
            return ExitCode::FAILURE;
        }
    };

    match run(&invocation) {
        Ok(Outcome::Success) => ExitCode::SUCCESS,
        Ok(Outcome::AppExit(status)) => {
            if status == 0 {
                ExitCode::SUCCESS
            } else if let Ok(code) = u8::try_from(status) {
                ExitCode::from(code)
            } else {
                // Windows process statuses are wider than `ExitCode`'s portable
                // `u8`. Forward via `exit` so the original value is preserved.
                process::exit(status);
            }
        }
        Err(error) => {
            write_line(&mut io::stderr(), &format!("Error: {error}"));
            ExitCode::FAILURE
        }
    }
}

/// Writes one line, giving up quietly if the stream will not take it.
///
/// The exit status is what a caller acts on, and a consumer that closed the
/// pipe has already stopped reading, so a failed write is not worth unwinding
/// the way the `print!` family would.
#[cfg(windows)]
fn write_line(stream: &mut impl Write, message: &str) {
    _ = writeln!(stream, "{message}");
    _ = stream.flush();
}
