//! Rendering of stored paths for people rather than for comparison.

use std::path::Path;

/// Renders a canonical stored path so a person can read and retype it.
///
/// Launch directories are stored canonicalized, which on Windows means the
/// extended-length form (`\\?\C:\work`). That prefix exists to lift path-length
/// and parsing limits, carries no meaning for a reader, and cannot be pasted
/// back into a shell, so it is dropped here.
///
/// This is the canonical path made readable, not the path as the user reached
/// it: canonicalization has already resolved junctions, links, and relative
/// spellings, and none of that is recoverable afterwards. Auto-detect compares
/// the stored canonical paths, so what is shown never decides which session
/// `resume` finds. Ref: docs/design.md, "Listing sessions".
pub(crate) fn display_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if let Some(share) = text.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{share}");
    }
    text.strip_prefix(r"\\?\").unwrap_or(&text).to_string()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn strips_the_extended_length_prefix() {
        assert_eq!(display_path(&PathBuf::from(r"\\?\C:\Source")), r"C:\Source");
    }

    #[test]
    fn restores_the_network_share_form() {
        assert_eq!(
            display_path(&PathBuf::from(r"\\?\UNC\server\share\work")),
            r"\\server\share\work"
        );
    }

    #[test]
    fn leaves_an_ordinary_path_alone() {
        assert_eq!(display_path(&PathBuf::from(r"C:\Source")), r"C:\Source");
        assert_eq!(display_path(&PathBuf::from("/work")), "/work");
    }
}
