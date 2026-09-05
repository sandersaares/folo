//! Default session store root.

use std::path::PathBuf;

use windows::Win32::System::Com::CoTaskMemFree;
use windows::Win32::UI::Shell::{FOLDERID_LocalAppData, KF_FLAG_DEFAULT, SHGetKnownFolderPath};

use crate::constants::STORE_SUBDIR;
use crate::pal::error::{PalError, PalErrorKind};

/// Per-user `LocalAppData` subdirectory `dure`, or a test override.
pub(crate) fn resolve_store_root(override_root: Option<PathBuf>) -> Result<PathBuf, PalError> {
    if let Some(root) = override_root {
        return Ok(root);
    }
    default_store_root()
}

// Thin Win32 known-folder binding: it reads a machine-specific path from the
// shell API, so there is no outcome a test could assert on.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
fn default_store_root() -> Result<PathBuf, PalError> {
    windows_local_app_data().map(|root| root.join(STORE_SUBDIR))
}

// Thin Win32 known-folder binding; see `default_store_root`.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
fn windows_local_app_data() -> Result<PathBuf, PalError> {
    // SAFETY: `FOLDERID_LocalAppData` is a well-known folder id. On success the
    // returned PWSTR is a non-null NUL-terminated string we own and must free
    // with CoTaskMemFree. No other alias of this allocation exists.
    let pwstr = unsafe { SHGetKnownFolderPath(&FOLDERID_LocalAppData, KF_FLAG_DEFAULT, None) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    let path = {
        // SAFETY: `pwstr` is the unique owner of a valid NUL-terminated path
        // string returned by SHGetKnownFolderPath. Creating a temporary `&[u16]`
        // via `PWSTR::as_wide` does not create a conflicting exclusive borrow;
        // we copy into a PathBuf before freeing.
        let wide = unsafe { pwstr.as_wide() };
        String::from_utf16(wide)
            .map(PathBuf::from)
            .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?
    };
    // SAFETY: `pwstr` is the pointer SHGetKnownFolderPath allocated and we have
    // finished copying it. CoTaskMemFree is the required deallocator.
    unsafe {
        CoTaskMemFree(Some(pwstr.0.cast()));
    }
    Ok(path)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn override_root_is_used_verbatim() {
        let root = PathBuf::from("/tmp/dure-store");
        assert_eq!(resolve_store_root(Some(root.clone())).unwrap(), root);
    }

    #[test]
    // Talks to the real operating system: reads the per-user known folder.
    #[cfg_attr(miri, ignore)]
    fn without_an_override_the_default_root_is_used() {
        let root = resolve_store_root(None).unwrap();
        assert!(root.ends_with(STORE_SUBDIR));
    }
}
