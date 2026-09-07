//! Child process driven by the `dure` Windows integration tests.
//!
//! Each subcommand parks the process in one observable state — waiting on
//! console input, reporting whether it sees a console, exiting with a chosen
//! status — so a test can drive the state a scenario needs and assert on it.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

#[cfg(windows)]
use std::io::{self, IsTerminal, Read, Write};
#[cfg(windows)]
use std::{env, fs, process};

/// Enables terminal focus, basic mouse, and SGR mouse reporting.
///
/// These are the protocols represented by `SAMPLE_TERMINAL_INPUT`, so the
/// helper negotiates them before asking the test terminal to send the sample.
#[cfg(windows)]
const ENABLE_TERMINAL_INPUT: &str = "\x1b[?1004h\x1b[?1000h\x1b[?1006h";

/// The helper serves Windows integration tests only, so on other platforms the
/// binary is an empty stub, matching `dure` itself
/// (`dure/docs/implementation.md`, "Platform gate").
#[cfg(not(windows))]
fn main() {}

#[cfg(windows)]
fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("echo-line") => {
            let mut line = String::new();
            io::stdin().read_line(&mut line).expect("read stdin");
            print!("{line}");
            io::stdout().flush().expect("flush stdout");
        }
        Some("print-and-wait") => {
            println!("ready");
            io::stdout().flush().expect("flush stdout");
            wait_for_byte();
        }
        Some("report-size-after-resize") => {
            print_console_size();
            wait_for_resize();
            print_console_size();
        }
        Some("report-size-after-input-and-resize") => {
            print_console_size();
            wait_for_key_then_flush_input();
            println!("{}", dure_test_helper::RESIZE_READY);
            io::stdout().flush().expect("flush stdout");
            wait_for_resize();
            print_console_size();
        }
        Some("verify-terminal-input") => {
            enable_vt_input();
            println!(
                "{ENABLE_TERMINAL_INPUT}{}",
                dure_test_helper::TERMINAL_INPUT_READY
            );
            io::stdout().flush().expect("flush stdout");
            let expected = dure_test_helper::SAMPLE_TERMINAL_INPUT;
            let mut actual = vec![0_u8; expected.len()];
            io::stdin()
                .read_exact(&mut actual)
                .expect("read terminal input sample");
            assert_eq!(actual, expected, "terminal input sample");
            println!("{}", dure_test_helper::TERMINAL_INPUT_OK);
            io::stdout().flush().expect("flush stdout");
        }
        Some("exit") => {
            process::exit(exit_code(&args));
        }
        Some("has-console") => {
            print_console_status();
        }
        Some("print-non-ascii") => {
            println!("{}", dure_test_helper::SAMPLE_NON_ASCII_TEXT);
            io::stdout().flush().expect("flush stdout");
        }
        Some("wait-has-console") => {
            wait_for_byte();
            print_console_status();
        }
        Some("wait-exit") => {
            wait_for_byte();
            process::exit(exit_code(&args));
        }
        _ => {
            eprintln!(
                "usage: dure-test-helper echo-line | print-and-wait | exit [code] | \
                 has-console | print-non-ascii | report-size-after-input-and-resize | \
                 report-size-after-resize | verify-terminal-input | wait-has-console | \
                 wait-exit [code]"
            );
            process::exit(2);
        }
    }
}

/// The status the `exit` and `wait-exit` subcommands terminate with.
#[cfg(windows)]
fn exit_code(args: &[String]) -> i32 {
    args.get(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// Blocks until console input arrives, modelling an app parked on the user.
#[cfg(windows)]
fn wait_for_byte() {
    let mut buf = [0_u8; 1];
    _ = io::stdin().read(&mut buf);
}

/// Configures the helper to receive terminal reports as VT bytes.
#[cfg(windows)]
fn enable_vt_input() {
    use windows::Win32::System::Console::{
        CONSOLE_MODE, ENABLE_ECHO_INPUT, ENABLE_LINE_INPUT, ENABLE_PROCESSED_INPUT,
        ENABLE_VIRTUAL_TERMINAL_INPUT, GetConsoleMode, GetStdHandle, STD_INPUT_HANDLE,
        SetConsoleMode,
    };

    // SAFETY: asks for this process's own standard input handle.
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) }.expect("std input handle");
    let mut mode = CONSOLE_MODE::default();
    // SAFETY: `handle` is this process's console input and `mode` outlives the call.
    unsafe { GetConsoleMode(handle, &raw mut mode) }.expect("console input mode");
    let vt_mode = CONSOLE_MODE(
        (mode.0 & !(ENABLE_ECHO_INPUT.0 | ENABLE_LINE_INPUT.0 | ENABLE_PROCESSED_INPUT.0))
            | ENABLE_VIRTUAL_TERMINAL_INPUT.0,
    );
    // SAFETY: `handle` remains this process's live console input handle, and
    // `vt_mode` combines documented input flags.
    unsafe { SetConsoleMode(handle, vt_mode) }.expect("enable VT input");
}

/// Waits for a key record, then removes every record queued before readiness.
#[cfg(windows)]
fn wait_for_key_then_flush_input() {
    use windows::Win32::System::Console::{
        FlushConsoleInputBuffer, GetStdHandle, INPUT_RECORD, KEY_EVENT, ReadConsoleInputW,
        STD_INPUT_HANDLE,
    };

    // SAFETY: asks for this process's own standard input handle.
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) }.expect("std input handle");
    loop {
        let mut records = [INPUT_RECORD::default()];
        let mut read = 0_u32;
        // SAFETY: `records` is exclusive and outlives the call.
        unsafe { ReadConsoleInputW(handle, &mut records, &raw mut read) }
            .expect("read console input");
        let record = records
            .first()
            .expect("the fixed input-record buffer has one element");
        if read != 0 && u32::from(record.EventType) == KEY_EVENT {
            break;
        }
    }
    // SAFETY: `handle` remains this process's live console input handle.
    unsafe { FlushConsoleInputBuffer(handle) }.expect("flush console input");
}

/// Blocks until the app's console reports a resize.
#[cfg(windows)]
fn wait_for_resize() {
    use windows::Win32::System::Console::{
        CONSOLE_MODE, ENABLE_WINDOW_INPUT, GetConsoleMode, GetStdHandle, INPUT_RECORD,
        ReadConsoleInputW, STD_INPUT_HANDLE, SetConsoleMode, WINDOW_BUFFER_SIZE_EVENT,
    };

    // SAFETY: asks for this process's own standard input handle.
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) }.expect("std input handle");
    let mut mode = CONSOLE_MODE::default();
    // SAFETY: `handle` is this process's console input and `mode` outlives the call.
    unsafe { GetConsoleMode(handle, &raw mut mode) }.expect("console input mode");
    // SAFETY: `handle` is this process's console input; the new mode preserves
    // every existing bit and adds resize notifications.
    unsafe { SetConsoleMode(handle, CONSOLE_MODE(mode.0 | ENABLE_WINDOW_INPUT.0)) }
        .expect("enable window input");

    loop {
        let mut records = [INPUT_RECORD::default()];
        let mut read = 0_u32;
        // SAFETY: `records` is exclusive and outlives the call.
        unsafe { ReadConsoleInputW(handle, &mut records, &raw mut read) }
            .expect("read console input");
        let record = records
            .first()
            .expect("the fixed input-record buffer has one element");
        if read != 0 && u32::from(record.EventType) == WINDOW_BUFFER_SIZE_EVENT {
            return;
        }
    }
}

/// Reports the console geometry the app currently sees.
///
/// Printed in a shape a test can find after the console host has wrapped the
/// line, so the numbers are joined to their label rather than sitting beside
/// it.
#[cfg(windows)]
fn print_console_size() {
    use windows::Win32::System::Console::{
        CONSOLE_SCREEN_BUFFER_INFO, GetConsoleScreenBufferInfo, GetStdHandle, STD_OUTPUT_HANDLE,
    };

    // SAFETY: asks for this process's own standard output handle, which the
    // call either returns or reports a failure for.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) }.expect("std output handle");
    let mut info = CONSOLE_SCREEN_BUFFER_INFO::default();
    // SAFETY: `handle` is this process's console output and `info` is a valid
    // stack structure that outlives the call.
    unsafe { GetConsoleScreenBufferInfo(handle, &raw mut info) }.expect("console screen info");
    let window = info.srWindow;
    let cols = i32::from(window.Right)
        .saturating_sub(i32::from(window.Left))
        .saturating_add(1);
    let rows = i32::from(window.Bottom)
        .saturating_sub(i32::from(window.Top))
        .saturating_add(1);
    println!("size:{cols}x{rows}");
    io::stdout().flush().expect("flush stdout");
}

/// Reports whether this process was given a real console or redirected pipes.
///
/// The result also goes to a file in the working directory so a test can read
/// it without scraping pseudoconsole output for text the console host is free
/// to reflow.
#[cfg(windows)]
fn print_console_status() {
    let status = if io::stdin().is_terminal() {
        "console"
    } else {
        "pipes"
    };
    fs::write("console-status.txt", status).expect("write console status");
    println!("{status}");
    io::stdout().flush().expect("flush stdout");
}
