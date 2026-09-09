use std::cmp::Reverse;

use ohno::AppError;

use crate::evidence::require;

/// Portable label for collector-local storage, which is not an execution observation.
///
/// The caller's per-check gap prefix identifies the observation's scope. Collector directories
/// are replaced without assigning machine-dependent names or ordinals to their replacements.
const LOCAL_PATH: &str = "<reporter-local>";

pub(crate) fn normalize_gap_paths(
    gaps: &mut [String],
    prefixes: Vec<String>,
) -> Result<(), AppError> {
    let mut prefixes: Vec<_> = prefixes
        .into_iter()
        .map(|prefix| prefix.trim_end_matches(['/', '\\']).to_owned())
        .collect();
    for prefix in &prefixes {
        require(
            absolute_path_text(prefix) && !prefix.contains(['\r', '\n', '\0']),
            "transient path prefixes must be non-root absolute directory paths",
        )?;
    }
    // Nested collector directories can themselves contain random names. Consume the longest
    // known prefix first so their varying suffixes do not survive a broader-root replacement.
    prefixes.sort_by_key(|prefix| (Reverse(prefix.len()), prefix.clone()));
    prefixes.dedup();
    for gap in gaps {
        for prefix in &prefixes {
            *gap = replace_directory(gap, prefix);
        }
    }
    Ok(())
}

fn absolute_path_text(prefix: &str) -> bool {
    if prefix.starts_with('/') {
        return true;
    }
    if let Some(tail) = prefix.strip_prefix("\\\\") {
        return tail.split('\\').filter(|part| !part.is_empty()).count() >= 2;
    }
    let bytes = prefix.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && bytes.get(1) == Some(&b':')
        && bytes.get(2).is_some_and(|byte| b"\\/".contains(byte))
}

fn replace_directory(text: &str, prefix: &str) -> String {
    let mut output = String::new();
    let mut copied_until = 0;
    for (start, matched) in text.match_indices(prefix) {
        let end = start
            .checked_add(matched.len())
            .expect("the matched range lies within the input string");
        let before = text
            .get(..start)
            .expect("match_indices produces UTF-8 boundaries")
            .chars()
            .next_back();
        let after = text
            .get(end..)
            .expect("match_indices produces UTF-8 boundaries")
            .chars()
            .next();
        if before.is_none_or(left_boundary) && after.is_none_or(right_boundary) {
            output.push_str(
                text.get(copied_until..start)
                    .expect("non-overlapping matches are in increasing UTF-8 order"),
            );
            output.push_str(LOCAL_PATH);
            copied_until = end;
        }
    }
    output.push_str(
        text.get(copied_until..)
            .expect("the last matched boundary lies within the input string"),
    );
    output
}

fn left_boundary(character: char) -> bool {
    character.is_whitespace() || "'\"`([{=:".contains(character)
}

fn right_boundary(character: char) -> bool {
    character.is_whitespace() || "/\\'\"`)]}:,;".contains(character)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::canonical::{digest, json};
    use crate::evidence::InvalidRecordError;

    #[test]
    fn retries_have_identical_gap_digests_and_preserve_scope() {
        let first = normalize(
            r"unit: cannot read 'C:\runner\first\jobs-random-a\check.log'",
            vec![r"C:\runner\first", r"C:\runner\first\jobs-random-a"],
        );
        let second = normalize(
            r"unit: cannot read 'D:\runner\second\jobs-random-b\check.log'",
            vec![
                r"D:\runner\second\jobs-random-b",
                r"D:\runner\second",
                r"D:\runner\second",
            ],
        );
        assert_eq!(first, r"unit: cannot read '<reporter-local>\check.log'");
        assert_eq!(
            digest(&json(&first).unwrap()),
            digest(&json(&second).unwrap())
        );
    }

    #[test]
    fn normalization_uses_literal_case_sensitive_directory_boundaries() {
        let gap = normalize(
            r#"C:\work\sibling 'C:\worker\file' 'c:\work\file' C:\work "C:\work""#,
            vec![r"C:\work\"],
        );
        assert_eq!(
            gap,
            r#"<reporter-local>\sibling 'C:\worker\file' 'c:\work\file' <reporter-local> "<reporter-local>""#
        );
    }

    #[test]
    fn linux_and_unc_paths_are_data_on_every_platform() {
        assert_eq!(
            normalize("read /runner/a/check.log", vec!["/runner/a/"]),
            "read <reporter-local>/check.log"
        );
        assert_eq!(
            normalize(
                r"read \\host\share\job\check.log",
                vec![r"\\host\share\job"]
            ),
            r"read <reporter-local>\check.log"
        );
    }

    #[test]
    fn unrelated_embedded_paths_are_unchanged() {
        let text = "see https://example.test/runner/check.log and prefix/runner/check.log";
        assert_eq!(normalize(text, vec!["/runner"]), text);
    }

    #[test]
    fn empty_configuration_and_reapplication_preserve_every_gap() {
        let mut gaps = vec!["same".to_owned(), "same".to_owned()];
        normalize_gap_paths(&mut gaps, Vec::new()).unwrap();
        assert_eq!(gaps, ["same", "same"]);
        let once = normalize("/runner/check.log", vec!["/runner"]);
        assert_eq!(normalize(&once, vec!["/runner"]), once);
    }

    #[test]
    fn invalid_prefixes_are_rejected_without_changing_gaps() {
        for prefix in ["", "/", "\\", "C:\\", "relative", "/runner\npath"] {
            let mut gaps = vec!["unmodified".to_owned()];
            let error = normalize_gap_paths(&mut gaps, vec![prefix.to_owned()]).unwrap_err();
            _ = error.find_source::<InvalidRecordError>().unwrap();
            assert_eq!(gaps, ["unmodified"]);
        }
    }

    fn normalize(gap: &str, prefixes: Vec<&str>) -> String {
        let mut gaps = [gap.to_owned()];
        normalize_gap_paths(&mut gaps, prefixes.into_iter().map(str::to_owned).collect()).unwrap();
        gaps.into_iter().next().unwrap()
    }
}
