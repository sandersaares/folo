//! The write-once configuration writer port used by `install`: it creates a new
//! configuration file but never clobbers one that already exists. The production
//! adapter is backed by `tokio::fs`; tests drive an in-memory fake.

use std::future::Future;
use std::io;
use std::path::Path;

use tokio::io::AsyncWriteExt;

/// Writes a new configuration file without overwriting an existing one.
pub(crate) trait ConfigWriter {
    /// Writes `contents` to `path`, creating parent directories as needed.
    ///
    /// Returns `Ok(true)` when the file was created, `Ok(false)` when a file
    /// already existed at `path` (and was left untouched), and `Err` on any other
    /// I/O failure.
    fn write_new(&self, path: &Path, contents: &str) -> impl Future<Output = io::Result<bool>>;
}

/// The production [`ConfigWriter`], backed by `tokio::fs`.
#[derive(Debug, Default)]
pub(crate) struct TokioConfigWriter;

impl ConfigWriter for TokioConfigWriter {
    async fn write_new(&self, path: &Path, contents: &str) -> io::Result<bool> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        // `create_new` makes the open fail rather than clobber an existing file,
        // upholding the never-overwrite contract.
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
        {
            Ok(mut file) => {
                file.write_all(contents.as_bytes()).await?;
                // Tokio's `File` does not flush on drop, so flush explicitly to
                // guarantee the bytes reach the filesystem before any later read.
                file.flush().await?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
pub(crate) use fake::MemoryConfigWriter;

#[cfg(test)]
mod fake {
    use std::collections::HashMap;
    use std::future::{Future, ready};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use super::{ConfigWriter, io};

    /// An in-memory [`ConfigWriter`] that records written files without touching
    /// the filesystem, so orchestration tests run under Miri.
    #[derive(Debug, Default)]
    pub(crate) struct MemoryConfigWriter {
        files: Mutex<HashMap<PathBuf, String>>,
        failure: Option<io::ErrorKind>,
    }

    impl MemoryConfigWriter {
        /// A writer pre-seeded with a file already present at `path`, so the next
        /// `write_new` reports `Ok(false)`.
        pub(crate) fn with_existing(path: &Path, contents: &str) -> Self {
            let writer = Self::default();
            writer
                .files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), contents.to_owned());
            writer
        }

        /// A writer that fails every write with an injected I/O error.
        pub(crate) fn failing() -> Self {
            Self {
                files: Mutex::default(),
                failure: Some(io::ErrorKind::Other),
            }
        }

        /// The contents recorded for `path`, if any.
        pub(crate) fn written(&self, path: &Path) -> Option<String> {
            self.files.lock().unwrap().get(path).cloned()
        }
    }

    impl ConfigWriter for MemoryConfigWriter {
        fn write_new(&self, path: &Path, contents: &str) -> impl Future<Output = io::Result<bool>> {
            let result = if let Some(kind) = self.failure {
                Err(io::Error::from(kind))
            } else {
                let mut files = self.files.lock().unwrap();
                if files.contains_key(path) {
                    Ok(false)
                } else {
                    files.insert(path.to_path_buf(), contents.to_owned());
                    Ok(true)
                }
            };
            ready(result)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod real_writer_tests {
    use tempfile::tempdir;

    use super::{ConfigWriter, TokioConfigWriter};

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_new_creates_missing_parent_directories() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested/.cargo/bench_history.toml");

        let written = TokioConfigWriter.write_new(&path, "payload").await.unwrap();

        assert!(written);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "payload");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_new_leaves_an_existing_file_untouched() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench_history.toml");
        std::fs::write(&path, "original").unwrap();

        let written = TokioConfigWriter
            .write_new(&path, "replacement")
            .await
            .unwrap();

        assert!(!written);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "original");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_new_with_a_blocked_parent_reports_an_error() {
        let dir = tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "x").unwrap();
        // "blocker" is a file, so it cannot also serve as a parent directory.
        let path = blocker.join("bench_history.toml");

        let result = TokioConfigWriter.write_new(&path, "payload").await;

        assert!(result.is_err(), "{result:?}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_new_maps_a_non_existence_open_error_to_io() {
        let dir = tempdir().unwrap();
        // An interior NUL survives path construction but the OS rejects it at open
        // time with `InvalidInput` (not `AlreadyExists`), exercising the generic
        // I/O arm rather than the already-exists arm.
        let path = dir.path().join("bad\0name");

        let error = TokioConfigWriter
            .write_new(&path, "payload")
            .await
            .unwrap_err();

        assert_ne!(error.kind(), std::io::ErrorKind::AlreadyExists, "{error:?}");
    }
}
