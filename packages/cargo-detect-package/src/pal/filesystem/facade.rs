// Facade that dispatches to either the real filesystem or a mock in tests.
//
// The facade pattern allows the same code to work with both real and mock implementations,
// with the mock variant only available in test builds.

use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;
use std::{fmt, io};

#[cfg(test)]
use crate::pal::MockFilesystem;
use crate::pal::{BuildTargetFilesystem, Filesystem};

/// Facade over filesystem operations, dispatching to real or mock implementation.
///
/// In production, this always uses `BuildTargetFilesystem`. In tests, it can also wrap a
/// `MockFilesystem` for controlled test scenarios.
#[derive(Clone)]
pub(crate) enum FilesystemFacade {
    /// Real filesystem implementation.
    Target(&'static BuildTargetFilesystem),

    /// Mock filesystem for testing.
    #[cfg(test)]
    Mock(Arc<MockFilesystem>),
}

// Debug implementations have no API contract to test.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for FilesystemFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(_) => f.debug_struct("FilesystemFacade::Target").finish(),
            #[cfg(test)]
            Self::Mock(_) => f.debug_struct("FilesystemFacade::Mock").finish(),
        }
    }
}

/// Static instance of the real filesystem for production use.
static BUILD_TARGET_FILESYSTEM: BuildTargetFilesystem = BuildTargetFilesystem;

impl FilesystemFacade {
    /// Creates a facade using the real filesystem.
    // A default-return mutation recurses through `Default::default()` instead of failing cleanly.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) const fn target() -> Self {
        Self::Target(&BUILD_TARGET_FILESYSTEM)
    }

    /// Creates a facade wrapping a mock filesystem (test builds only).
    #[cfg(test)]
    // Facade pass-through logic is not worth testing.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn from_mock(mock: MockFilesystem) -> Self {
        Self::Mock(Arc::new(mock))
    }
}

// Facade types are trivial pass-through layers - not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Filesystem for FilesystemFacade {
    fn cargo_toml_exists(&self, dir: &Path) -> bool {
        match self {
            Self::Target(fs) => fs.cargo_toml_exists(dir),
            #[cfg(test)]
            Self::Mock(mock) => mock.cargo_toml_exists(dir),
        }
    }

    fn read_cargo_toml(&self, dir: &Path) -> io::Result<String> {
        match self {
            Self::Target(fs) => fs.read_cargo_toml(dir),
            #[cfg(test)]
            Self::Mock(mock) => mock.read_cargo_toml(dir),
        }
    }

    fn is_file(&self, path: &Path) -> bool {
        match self {
            Self::Target(fs) => fs.is_file(path),
            #[cfg(test)]
            Self::Mock(mock) => mock.is_file(path),
        }
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        match self {
            Self::Target(fs) => fs.canonicalize(path),
            #[cfg(test)]
            Self::Mock(mock) => mock.canonicalize(path),
        }
    }

    fn current_dir(&self) -> io::Result<PathBuf> {
        match self {
            Self::Target(fs) => fs.current_dir(),
            #[cfg(test)]
            Self::Mock(mock) => mock.current_dir(),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        match self {
            Self::Target(fs) => fs.exists(path),
            #[cfg(test)]
            Self::Mock(mock) => mock.exists(path),
        }
    }
}

// Facade types are trivial pass-through layers - not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Default for FilesystemFacade {
    fn default() -> Self {
        Self::target()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;

    use super::*;

    #[test]
    fn target_constructor_executes_at_runtime() {
        assert!(matches!(
            black_box(FilesystemFacade::target()),
            FilesystemFacade::Target(_)
        ));
    }
}
