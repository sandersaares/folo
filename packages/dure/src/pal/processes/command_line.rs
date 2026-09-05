//! Windows `CreateProcessW` command-line construction.
//!
//! `CreateProcessW` takes one string that `CommandLineToArgvW` later splits.
//! Each argv element is quoted so spaces and embedded quotes survive that split.
//! See <https://learn.microsoft.com/windows/win32/api/shellapi/nf-shellapi-commandlinetoargvw>.

use crate::app_command::quote_windows_arg;

/// Builds a Windows process command line from an executable and following argv.
#[must_use]
pub(crate) fn windows_command_line(exe: &str, args: &[String]) -> String {
    let mut line = quote_windows_arg(exe);
    for arg in args {
        line.push(' ');
        line.push_str(&quote_windows_arg(arg));
    }
    line
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn each_element_survives_the_split_that_follows() {
        assert_eq!(
            windows_command_line("app.exe", &["--foo".to_string()]),
            "app.exe --foo"
        );
        assert_eq!(
            windows_command_line(r"C:\Program Files\app.exe", &["a b".to_string()]),
            r#""C:\Program Files\app.exe" "a b""#
        );
    }
}
