//! Fixture for the Windows integration tests.
//!
//! The helper is a separate executable with its own accepted modes and its own
//! rules for how a waiting mode is released. Spelling those out at each call
//! site spreads one protocol across unrelated tests, so they live here: a test
//! says which scenario it wants and this module knows how to run it.

use std::path::{Path, PathBuf};

use dure::test_support::ConsoleProcess;
use dure_test_helper::binary_path;

/// Arbitrary nonzero status used to prove forwarding, not a special value.
pub(crate) const SAMPLE_NONZERO_EXIT: i32 = 7;

/// The id a fresh store hands to its first session, so a test that owns an
/// empty store knows the banner to expect.
pub(crate) const FIRST_SESSION_ID: u64 = 1;

/// What a helper parked on console input prints before it starts waiting.
///
/// A test that needs the session running can wait for this instead of for
/// something `dure` prints, so a regression in `dure`'s own output fails an
/// assertion rather than leaving the helper parked forever.
pub(crate) const HELPER_READY: &str = "ready";

/// Distinctive part of the helper's file name, which a verbose run echoes and a
/// quiet one never does.
pub(crate) const HELPER_STEM: &str = "dure-test-helper";

/// The bytes that release a helper parked on console input.
///
/// The helper's console is in line-input mode, so a lone character is not
/// delivered to a `read` until a newline arrives.
pub(crate) const WAKEUP: &[u8] = b"x\r\n";

pub(crate) fn dure_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dure"))
}

pub(crate) fn helper_exe() -> PathBuf {
    binary_path().to_path_buf()
}

/// A state the helper can be asked to park in or run through.
///
/// Naming the modes here is what keeps a rename of the helper's command line
/// from turning into several integration tests failing on missing output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scenario {
    /// Reads one line and echoes it back.
    EchoLine,
    /// Says it is ready, then waits for input.
    PrintAndWait,
    /// Prints text no ASCII-only path can carry, then exits.
    PrintNonAscii,
    /// Reports its console size, waits for a resize, then reports it again.
    ReportSizeAfterResize,
    /// Waits for input, then reports whether it has a console.
    WaitThenReportConsole,
    /// Waits for input, then exits with `SAMPLE_NONZERO_EXIT`.
    WaitThenExit,
}

impl Scenario {
    fn argv(self) -> Vec<String> {
        match self {
            Self::EchoLine => vec!["echo-line".to_string()],
            Self::PrintAndWait => vec!["print-and-wait".to_string()],
            Self::PrintNonAscii => vec!["print-non-ascii".to_string()],
            Self::ReportSizeAfterResize => vec!["report-size-after-resize".to_string()],
            Self::WaitThenReportConsole => vec!["wait-has-console".to_string()],
            Self::WaitThenExit => {
                vec!["wait-exit".to_string(), SAMPLE_NONZERO_EXIT.to_string()]
            }
        }
    }
}

/// A `dure` invocation, built rather than spelled out.
pub(crate) struct DureCommand {
    args: Vec<String>,
}

impl DureCommand {
    /// `dure run -- <helper> <scenario>` against `store_root`.
    pub(crate) fn run(store_root: &Path, scenario: Scenario) -> Self {
        let mut args = store_root_args(store_root);
        args.push("run".to_string());
        args.push("--".to_string());
        args.push(helper_exe().display().to_string());
        args.extend(scenario.argv());
        Self { args }
    }

    /// Bare `dure resume`, which finds the session by launch directory.
    pub(crate) fn resume(store_root: &Path) -> Self {
        let mut args = store_root_args(store_root);
        args.push("resume".to_string());
        Self { args }
    }

    /// Explains what it is doing as it goes.
    #[must_use]
    pub(crate) fn verbose(mut self) -> Self {
        let at = self
            .args
            .iter()
            .position(|arg| arg == "--")
            .expect("a verbose flag belongs before the app command");
        self.args.insert(at, "--verbose".to_string());
        self
    }

    /// Starts this command in its own console, from `launch_directory`.
    pub(crate) fn spawn(&self, launch_directory: &Path) -> ConsoleProcess {
        ConsoleProcess::spawn(&dure_exe(), &self.args, launch_directory)
    }

    /// Starts it under a job that forbids breakaway, as `cargo run` would.
    pub(crate) fn spawn_confined(&self, launch_directory: &Path) -> ConsoleProcess {
        ConsoleProcess::spawn_confined(&dure_exe(), &self.args, launch_directory)
    }

    /// Starts it under an ancestor job it cannot leave.
    pub(crate) fn spawn_confined_by_ancestor(&self, launch_directory: &Path) -> ConsoleProcess {
        ConsoleProcess::spawn_confined_by_ancestor(&dure_exe(), &self.args, launch_directory)
    }
}

fn store_root_args(store_root: &Path) -> Vec<String> {
    vec!["--store-root".to_string(), store_root.display().to_string()]
}

/// Lets a helper parked on console input finish.
///
/// Sent before the terminal output is examined, so a scenario that no longer
/// prints what a test is looking for ends with that assertion failing rather
/// than with a helper waiting for input the test never sent.
pub(crate) fn release(client: &ConsoleProcess) {
    client.write_input(WAKEUP);
}

/// Names of everything the store root holds.
pub(crate) fn store_entries(store_root: &Path) -> Vec<String> {
    std::fs::read_dir(store_root)
        .expect("the store root is a directory this test created")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}
