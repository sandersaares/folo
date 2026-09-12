//! Harmless native cargo stand-in compiled by MutantsRecipe.Tests.ps1 using the existing rustc.
//!
//! Captures the recipe's argv and mutation environment without compiling helpers or testing
//! mutants. Line-oriented output is sufficient for the ordinary, single-line fixture arguments.

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut output =
        BufWriter::new(File::create(env::var_os("MUTANTS_FIXTURE_CAPTURE").unwrap()).unwrap());
    for argument in env::args().skip(1) {
        writeln!(output, "argument={argument}").unwrap();
    }
    // These are the environment settings the shared recipe prepares for cargo-mutants.
    for name in [
        "CARGO_TARGET_DIR",
        "TMP",
        "RUSTFLAGS",
        "RUST_TEST_THREADS",
        "MUTATION_TESTING",
        "CBH_FAKER",
        "DURE_TEST_HELPER",
    ] {
        writeln!(
            output,
            "environment:{name}={}",
            env::var(name).unwrap_or_default()
        )
        .unwrap();
    }
    // The PowerShell interpreter supplies its processor count for the recipe's concurrency rule.
    writeln!(
        output,
        "processor_count={}",
        env::var("MUTANTS_FIXTURE_PROCESSOR_COUNT").unwrap()
    )
    .unwrap();
    output.flush().unwrap();
    println!("mutation stdout canary");
    eprintln!("mutation stderr canary");
    ExitCode::from(
        env::var("MUTANTS_FIXTURE_EXIT")
            .unwrap()
            .parse::<u8>()
            .unwrap(),
    )
}
