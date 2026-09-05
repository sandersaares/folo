//! The command a session runs.

use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::pal::processes::quote_windows_arg;

/// An executable and the arguments it is launched with.
///
/// A session always runs something, so the executable is carried separately
/// from the arguments and is guaranteed to be there. That is what lets the
/// layers below — the supervisor bootstrap, the process PAL, and the session
/// record — consume a command without each re-deriving the same invariant from
/// an argv that might have been empty (design.md, "Commands").
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppCommand {
    exe: String,
    args: Vec<String>,
}

impl AppCommand {
    /// Takes an argv whose first element names the executable.
    ///
    /// Returns `None` for an argv that names nothing to run.
    #[must_use]
    pub fn from_argv(argv: Vec<String>) -> Option<Self> {
        let mut argv = argv.into_iter();
        let exe = argv.next()?;
        if exe.is_empty() {
            return None;
        }
        Some(Self {
            exe,
            args: argv.collect(),
        })
    }

    /// The executable to launch.
    #[must_use]
    pub fn exe(&self) -> &str {
        &self.exe
    }

    /// The arguments after the executable.
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// The whole command line as argv, executable first.
    #[must_use]
    pub fn argv(&self) -> Vec<String> {
        let mut argv = Vec::with_capacity(self.args.len().saturating_add(1));
        argv.push(self.exe.clone());
        argv.extend(self.args.iter().cloned());
        argv
    }

    /// A command for tests, from an argv that must name an executable.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test(argv: &[&str]) -> Self {
        Self::from_argv(argv.iter().map(|arg| (*arg).to_string()).collect())
            .expect("test argv names an executable")
    }
}

/// Renders the command the way a user could type it back.
///
/// Each token is quoted by the same rule Windows uses to split a command line,
/// so an argument holding spaces stays one argument on screen instead of
/// looking like several. Control characters are escaped because a terminal
/// would otherwise act on them: an argument holding a newline would split one
/// session across two rows of `dure list`, and an escape sequence would repaint
/// the screen (design.md, "Listing sessions").
impl fmt::Display for AppCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, token) in self.argv().iter().enumerate() {
            if index > 0 {
                f.write_str(" ")?;
            }
            for character in quote_windows_arg(token).chars() {
                if character.is_control() {
                    for escaped in character.escape_debug() {
                        write!(f, "{escaped}")?;
                    }
                } else {
                    write!(f, "{character}")?;
                }
            }
        }
        Ok(())
    }
}

/// Stored as a flat argv array, which is what the record file has always held.
impl Serialize for AppCommand {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.argv().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AppCommand {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let argv = Vec::<String>::deserialize(deserializer)?;
        Self::from_argv(argv).ok_or_else(|| D::Error::custom("a session command names an executable"))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn command(argv: &[&str]) -> AppCommand {
        AppCommand::for_test(argv)
    }

    #[test]
    fn an_argv_without_an_executable_is_refused() {
        assert!(AppCommand::from_argv(Vec::new()).is_none());
        assert!(AppCommand::from_argv(vec![String::new()]).is_none());
    }

    #[test]
    fn the_first_element_is_the_executable() {
        let command = command(&["app.exe", "--foo"]);
        assert_eq!(command.exe(), "app.exe");
        assert_eq!(command.args(), ["--foo"]);
        assert_eq!(command.argv(), ["app.exe", "--foo"]);
    }

    #[test]
    fn argument_boundaries_survive_being_displayed() {
        // Both of these join to the same text without quoting, so quoting is
        // what keeps two different launches from looking identical.
        let one_argument = command(&["tool.exe", r"C:\Program Files\tool.cfg"]);
        let two_arguments = command(&["tool.exe", r"C:\Program", r"Files\tool.cfg"]);
        assert_eq!(
            one_argument.to_string(),
            r#"tool.exe "C:\Program Files\tool.cfg""#
        );
        assert_eq!(
            two_arguments.to_string(),
            r"tool.exe C:\Program Files\tool.cfg"
        );
        assert_ne!(one_argument.to_string(), two_arguments.to_string());
    }

    #[test]
    fn an_empty_argument_is_visible() {
        assert_eq!(command(&["app.exe", ""]).to_string(), r#"app.exe """#);
    }

    #[test]
    fn a_control_character_cannot_reach_the_terminal() {
        let rendered = command(&["app.exe", "safe\nforged"]).to_string();
        assert!(!rendered.contains('\n'), "a newline survived in {rendered}");
        assert!(rendered.contains(r"\n"));
        let rendered = command(&["app.exe", "clear\u{1b}[2J"]).to_string();
        assert!(!rendered.contains('\u{1b}'), "an escape survived");
    }

    #[test]
    fn round_trips_as_a_flat_argv_array() {
        let command = command(&["app.exe", "--foo"]);
        let json = serde_json::to_string(&command).unwrap();
        assert_eq!(json, r#"["app.exe","--foo"]"#);
        assert_eq!(
            serde_json::from_str::<AppCommand>(&json).unwrap(),
            command
        );
    }

    #[test]
    fn a_stored_command_without_an_executable_is_refused() {
        serde_json::from_str::<AppCommand>("[]").unwrap_err();
    }
}
