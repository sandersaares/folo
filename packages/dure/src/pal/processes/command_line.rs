//! Windows `CreateProcessW` command-line construction.
//!
//! `CreateProcessW` takes one string that `CommandLineToArgvW` later splits.
//! Each argv element is quoted so spaces and embedded quotes survive that split.
//! See <https://learn.microsoft.com/windows/win32/api/shellapi/nf-shellapi-commandlinetoargvw>.
//!
//! The line is built as UTF-16 rather than as Rust text. A Windows path is a
//! sequence of UTF-16 code units that need not be valid Unicode, and narrowing
//! one to a `String` replaces what it cannot represent — which changes the path
//! rather than only how it looks. Quoting recognizes only ASCII, and no ASCII
//! code unit is part of a surrogate pair, so it applies to the encoded form
//! directly.

use std::ffi::OsStr;
use std::iter;
use std::os::windows::ffi::OsStrExt;

const QUOTE: u16 = b'"' as u16;
const BACKSLASH: u16 = b'\\' as u16;
const SPACE: u16 = b' ' as u16;
const TAB: u16 = b'\t' as u16;
const NEWLINE: u16 = b'\n' as u16;
const RETURN: u16 = b'\r' as u16;

/// Builds a Windows process command line from an executable and following argv.
#[must_use]
pub(crate) fn windows_command_line(exe: &OsStr, args: &[String]) -> Vec<u16> {
    let mut line = quote_wide(&encode(exe));
    for arg in args {
        line.push(SPACE);
        line.extend(quote_wide(&encode(OsStr::new(arg))));
    }
    line
}

fn encode(value: &OsStr) -> Vec<u16> {
    value.encode_wide().collect()
}

fn quote_wide(arg: &[u16]) -> Vec<u16> {
    if arg.is_empty() {
        return vec![QUOTE, QUOTE];
    }
    let needs_quotes = arg
        .iter()
        .any(|unit| matches!(*unit, SPACE | TAB | NEWLINE | RETURN | QUOTE));
    if !needs_quotes {
        return arg.to_vec();
    }

    let mut quoted = vec![QUOTE];
    let mut backslashes = 0_usize;
    for unit in arg {
        if *unit == BACKSLASH {
            backslashes = backslashes.saturating_add(1);
            continue;
        }
        if *unit == QUOTE {
            quoted.extend(iter::repeat_n(
                BACKSLASH,
                backslashes.saturating_mul(2).saturating_add(1),
            ));
            quoted.push(QUOTE);
            backslashes = 0;
            continue;
        }
        quoted.extend(iter::repeat_n(BACKSLASH, backslashes));
        quoted.push(*unit);
        backslashes = 0;
    }
    quoted.extend(iter::repeat_n(BACKSLASH, backslashes.saturating_mul(2)));
    quoted.push(QUOTE);
    quoted
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;

    use super::*;

    fn line(exe: &str, args: &[&str]) -> String {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        String::from_utf16(&windows_command_line(OsStr::new(exe), &args)).unwrap()
    }

    #[test]
    fn each_element_survives_the_split_that_follows() {
        assert_eq!(line("app.exe", &["--foo"]), "app.exe --foo");
        assert_eq!(
            line(r"C:\Program Files\app.exe", &["a b"]),
            r#""C:\Program Files\app.exe" "a b""#
        );
    }

    #[test]
    fn a_path_windows_gave_us_is_the_path_windows_gets_back() {
        // An unpaired surrogate: a legal Windows filename that no Rust string
        // can hold. Narrowing it would substitute U+FFFD, which names a
        // different file rather than the same one spelled differently.
        let exe = OsString::from_wide(&[
            u16::from(b'a'),
            0xD800,
            u16::from(b'.'),
            u16::from(b'e'),
            u16::from(b'x'),
            u16::from(b'e'),
        ]);
        assert!(
            windows_command_line(&exe, &[]).contains(&0xD800),
            "the code unit Windows gave us reached the command line"
        );
    }
}
