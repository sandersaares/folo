//! Formatting of `dure list` output.
//!
//! Ref: docs/design.md, "Listing sessions"; docs/implementation.md, "Output
//! rendering".

use std::time::Duration;

use crate::path_display::display_path;
use crate::session_record::SessionRecord;

/// Column headings, in the order they are printed.
///
/// The pid is the supervisor's and the directory is the launch directory that
/// auto-detect keys on, so the headings name both rather than leaving a reader
/// to guess which process or which directory a cell refers to.
const HEADERS: [&str; 6] = [
    "ID",
    "ATTACHED",
    "SUPERVISOR PID",
    "AGE",
    "LAUNCH DIRECTORY",
    "COMMAND",
];

/// Blank columns between one field and the next.
const COLUMN_GAP: usize = 2;

const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

/// Renders live sessions as a stable table.
///
/// Column widths follow the widest cell in each column, so a heading lines up
/// with what it labels whatever the sessions happen to contain. `now_unix_ms`
/// is passed in rather than read here so the rendering is a pure function of
/// its inputs.
#[must_use]
pub(crate) fn format_list(sessions: &[SessionRecord], now_unix_ms: u64) -> String {
    let rows: Vec<[String; 6]> = sessions
        .iter()
        .map(|session| row(session, now_unix_ms))
        .collect();
    let widths = column_widths(&rows);
    let mut lines = Vec::with_capacity(rows.len().saturating_add(1));
    lines.push(render_row(&HEADERS.map(str::to_string), &widths));
    for row in &rows {
        lines.push(render_row(row, &widths));
    }
    lines.join("\n")
}

fn row(session: &SessionRecord, now_unix_ms: u64) -> [String; 6] {
    [
        session.id.to_string(),
        if session.attached { "yes" } else { "no" }.to_string(),
        session.supervisor.pid.to_string(),
        format_age(session.started_at_unix_ms, now_unix_ms),
        printable(&display_path(&session.launch_directory)),
        session.command.to_string(),
    ]
}

/// Renders a cell so that it cannot break the table it sits in.
///
/// A command a user launched may carry control characters, and a terminal acts
/// on them: a newline would split one session across two rows and an escape
/// sequence would repaint the screen. Showing them in escaped form keeps a row
/// a row, and keeps the table a description of the sessions rather than
/// something they can drive.
fn printable(cell: &str) -> String {
    let mut rendered = String::with_capacity(cell.len());
    for character in cell.chars() {
        if character.is_control() {
            rendered.extend(character.escape_debug());
        } else {
            rendered.push(character);
        }
    }
    rendered
}

/// Renders how long a session has been running, coarsest unit first.
///
/// Two units are enough to tell sessions apart at a glance, and dropping the
/// rest keeps the column narrow. A clock that has moved backwards since the
/// session started reads as no elapsed time rather than as a negative age.
fn format_age(started_at_unix_ms: u64, now_unix_ms: u64) -> String {
    let seconds = Duration::from_millis(now_unix_ms.saturating_sub(started_at_unix_ms)).as_secs();
    let minutes = seconds.div_euclid(SECONDS_PER_MINUTE);
    let hours = minutes.div_euclid(MINUTES_PER_HOUR);
    let days = hours.div_euclid(HOURS_PER_DAY);
    if minutes == 0 {
        format!("{seconds}s")
    } else if hours == 0 {
        format!("{minutes}m")
    } else if days == 0 {
        format!("{hours}h{:02}m", minutes.rem_euclid(MINUTES_PER_HOUR))
    } else {
        format!("{days}d{:02}h", hours.rem_euclid(HOURS_PER_DAY))
    }
}

/// Widest cell per column, headings included.
fn column_widths(rows: &[[String; 6]]) -> [usize; 6] {
    let mut widths = HEADERS.map(cell_width);
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell_width(cell));
        }
    }
    widths
}

/// Columns are counted in Unicode scalar values.
///
/// Rendered width is a property of the terminal and its font: a wide CJK
/// character occupies two cells and a combining accent none, and terminals do
/// not agree on the ambiguous cases. A scalar count approximates that well
/// enough for a session list, and a byte count would not approximate it at all,
/// so alignment is exact for the paths and command lines people actually have
/// and best effort beyond them. Ref: docs/design.md, "Listing sessions".
fn cell_width(cell: &str) -> usize {
    cell.chars().count()
}

/// Pads every cell but the last, so no line carries trailing blanks.
fn render_row(cells: &[String; 6], widths: &[usize; 6]) -> String {
    let mut line = String::new();
    let last = cells.len().saturating_sub(1);
    for (index, cell) in cells.iter().enumerate() {
        line.push_str(cell);
        if index == last {
            break;
        }
        let width = widths.get(index).copied().unwrap_or_default();
        let padding = width
            .saturating_sub(cell_width(cell))
            .saturating_add(COLUMN_GAP);
        for _ in 0..padding {
            line.push(' ');
        }
    }
    line
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::session_record::ProcessIdentity;
    use crate::{AppCommand, SessionId};

    /// Sessions in these tests start at the epoch and are read at a fixed
    /// offset, so ages are chosen per test rather than inherited from a clock.
    const STARTED_AT: u64 = 0;

    /// An age with no interesting structure, for tests about other columns.
    const SOME_AGE_MS: u64 = 5 * 1000;

    /// An id for a fixture. Positive because every session id is.
    fn session_id(raw: u32) -> SessionId {
        SessionId::from_u32(raw).expect("fixture ids are positive")
    }

    fn session(id: SessionId, directory: &str, command: &str) -> SessionRecord {
        SessionRecord {
            id,
            supervisor: ProcessIdentity {
                pid: 99,
                creation_time: 1,
            },
            pipe_name: "pipe".to_string(),
            launch_directory: PathBuf::from(directory),
            command: AppCommand::for_test(&[command]),
            started_at_unix_ms: STARTED_AT,
            attached: true,
        }
    }

    #[test]
    fn empty_list_has_header_only() {
        let text = format_list(&[], SOME_AGE_MS);
        assert!(text.starts_with("ID  ATTACHED"));
        assert_eq!(text.lines().count(), 1);
    }

    #[test]
    fn includes_id_directory_and_command() {
        let mut only = session(session_id(4), "/work", "copilot.exe");
        only.command = AppCommand::for_test(&["copilot.exe", "--foo"]);
        let text = format_list(&[only], SOME_AGE_MS);
        assert!(text.contains('4'));
        assert!(text.contains("yes"));
        assert!(text.contains("99"));
        assert!(text.contains("/work"));
        assert!(text.contains("copilot.exe --foo"));
    }

    #[test]
    fn a_heading_starts_where_the_cell_it_labels_starts() {
        let text = format_list(
            &[session(session_id(1234567), "/work", "app.exe")],
            SOME_AGE_MS,
        );
        let lines: Vec<&str> = text.lines().collect();
        let header = lines.first().expect("a header line");
        let row = lines.get(1).expect("a session line");
        // The id is wider than its heading here, so every later column shifts
        // right and the headings have to shift with it.
        assert_eq!(
            header.find("ATTACHED").expect("the attached heading"),
            row.find("yes").expect("the attached cell")
        );
        assert_eq!(
            header
                .find("SUPERVISOR PID")
                .expect("the supervisor pid heading"),
            row.find("99").expect("the pid cell")
        );
        assert_eq!(
            header.find("AGE").expect("the age heading"),
            row.find("5s").expect("the age cell")
        );
        assert_eq!(
            header
                .find("LAUNCH DIRECTORY")
                .expect("the launch directory heading"),
            row.find("/work").expect("the directory cell")
        );
        assert_eq!(
            header.find("COMMAND").expect("the command heading"),
            row.find("app.exe").expect("the command cell")
        );
    }

    #[test]
    fn a_column_is_as_wide_as_the_widest_session_in_it() {
        let text = format_list(
            &[
                session(session_id(1), "/a", "app.exe"),
                session(session_id(2), "/a-much-longer-directory", "app.exe"),
            ],
            SOME_AGE_MS,
        );
        let lines: Vec<&str> = text.lines().collect();
        let header = lines.first().expect("a header line");
        let command = header.find("COMMAND").expect("the command heading");
        for row in lines.iter().skip(1) {
            assert_eq!(row.find("app.exe"), Some(command));
        }
    }

    #[test]
    fn directories_are_shown_without_the_extended_length_prefix() {
        let text = format_list(
            &[session(session_id(1), r"\\?\C:\Source", "app.exe")],
            SOME_AGE_MS,
        );
        assert!(text.contains(r"C:\Source"));
        assert!(!text.contains(r"\\?\"));
    }

    #[test]
    fn no_line_carries_trailing_blanks() {
        let text = format_list(
            &[
                session(
                    session_id(1),
                    r"\\?\C:\a-very-long-source-directory",
                    "copilot.exe",
                ),
                session(session_id(2), r"\\?\C:\b", "app.exe"),
            ],
            SOME_AGE_MS,
        );
        for line in text.lines() {
            assert_eq!(line, line.trim_end(), "trailing blanks in {line:?}");
        }
    }

    #[test]
    fn a_session_stays_one_row_however_its_command_was_written() {
        let text = format_list(
            &[session(
                session_id(1),
                "/work",
                "app.exe\nID  ATTACHED\n2  yes",
            )],
            SOME_AGE_MS,
        );
        // A heading line and exactly one session line.
        assert_eq!(text.lines().count(), 2);
        assert!(text.contains(r"app.exe\nID  ATTACHED\n2  yes"));
    }

    #[test]
    fn a_command_cannot_repaint_the_screen_it_is_listed_on() {
        let text = format_list(
            &[session(session_id(1), "/work", "app.exe\u{1b}[2J")],
            SOME_AGE_MS,
        );
        assert!(!text.contains('\u{1b}'), "an escape survived in {text:?}");
        assert!(text.contains(r"app.exe\u{1b}[2J"));
    }

    #[test]
    fn age_reports_the_coarsest_two_units() {
        assert_eq!(format_age(0, 0), "0s");
        assert_eq!(format_age(0, 45 * 1000), "45s");
        assert_eq!(format_age(0, 59 * 1000), "59s");
        assert_eq!(format_age(0, 60 * 1000), "1m");
        assert_eq!(format_age(0, 59 * 60 * 1000), "59m");
        assert_eq!(format_age(0, 60 * 60 * 1000), "1h00m");
        assert_eq!(format_age(0, (3 * 60 + 7) * 60 * 1000), "3h07m");
        assert_eq!(format_age(0, 24 * 60 * 60 * 1000), "1d00h");
        assert_eq!(format_age(0, (2 * 24 + 5) * 60 * 60 * 1000), "2d05h");
    }

    #[test]
    fn age_ignores_sub_second_precision() {
        assert_eq!(format_age(0, 999), "0s");
        assert_eq!(format_age(0, 1999), "1s");
    }

    #[test]
    fn a_session_stamped_in_the_future_has_no_age_rather_than_a_negative_one() {
        assert_eq!(format_age(SOME_AGE_MS, 0), "0s");
    }
}
