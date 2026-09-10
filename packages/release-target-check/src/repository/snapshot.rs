use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use ohno::AppError;

use crate::command::{capture, git};

/// Binds all verification reads to the caller's immutable candidate checkout.
#[derive(Debug)]
pub(crate) struct Repository {
    pub(crate) root: PathBuf,
    commit: String,
}

impl Repository {
    pub(crate) fn discover(manifest: &Path, commit: &str) -> Result<Self, AppError> {
        let manifest = canonicalize(manifest)?;
        let directory = manifest.parent().ok_or_else(|| {
            VerificationError::new("candidate manifest must have a parent directory")
        })?;
        let root = git(["rev-parse", "--show-toplevel"], directory)?;
        Ok(Self {
            root: canonicalize(Path::new(root.trim_end_matches(['\r', '\n'])))?,
            commit: commit.to_owned(),
        })
    }

    pub(crate) fn ensure_clean_head(&self) -> Result<(), AppError> {
        let head = git(["rev-parse", "--verify", "HEAD"], &self.root)?;
        if head.trim() != self.commit {
            return Err(VerificationError::new(format!(
                "candidate HEAD {} differs from requested commit {}",
                head.trim(),
                self.commit
            ))
            .into());
        }
        let status = capture(
            "git",
            [
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ],
            &self.root,
        )?;
        if !status.is_empty() {
            return Err(VerificationError::new(format!(
                "candidate checkout is not clean: {}",
                String::from_utf8_lossy(&status).replace('\0', "; ")
            ))
            .into());
        }
        // Status trusts assume-unchanged and skip-worktree flags. Those flags cannot certify
        // released source bytes, even when the index itself names the correct commit.
        let files = capture("git", ["ls-files", "-v", "-z"], &self.root)?;
        if files
            .split(|byte| *byte == b'\0')
            .filter(|entry| !entry.is_empty())
            .any(|entry| entry.first() != Some(&b'H'))
        {
            return Err(VerificationError::new(
                "candidate index contains flags that conceal tracked worktree changes",
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn checked<T>(
        &self,
        operation: impl FnOnce() -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        self.ensure_clean_head()?;
        let result = operation();
        // Recheck failed operations too: callers must not reuse evidence changed by a failed
        // metadata or checker invocation. The candidate is never repaired here.
        self.ensure_clean_head()?;
        result
    }

    pub(crate) fn ensure_first_parent(&self, release_line: &str) -> Result<(), AppError> {
        for commit in [&self.commit, release_line] {
            let resolved = git(
                [
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    &format!("{commit}^{{commit}}"),
                ],
                &self.root,
            )?;
            if resolved.trim() != commit {
                return Err(VerificationError::new(
                    "the supplied object ID must identify a commit",
                )
                .into());
            }
        }
        let history = git(
            ["rev-list", "--first-parent", release_line, "--"],
            &self.root,
        )?;
        if !history.lines().any(|commit| commit == self.commit) {
            return Err(VerificationError::new(format!(
                "candidate {} is not on the first-parent history of release line {release_line}",
                self.commit
            ))
            .into());
        }
        Ok(())
    }

    pub(crate) fn require_tracked(&self, path: &Path) -> Result<PathBuf, AppError> {
        let path = canonicalize(path)?;
        let relative = path.strip_prefix(&self.root).map_err(|error| {
            VerificationError::caused_by(
                format!(
                    "release input is outside the candidate repository: {}",
                    path.display()
                ),
                error,
            )
        })?;
        _ = git(
            [
                OsStr::new("--literal-pathspecs"),
                OsStr::new("ls-files"),
                OsStr::new("--error-unmatch"),
                OsStr::new("--"),
                relative.as_os_str(),
            ],
            &self.root,
        )?;
        Ok(path)
    }
}

pub(crate) fn canonicalize(path: &Path) -> Result<PathBuf, AppError> {
    fs::canonicalize(path).map_err(|error| {
        VerificationError::caused_by(
            format!("cannot locate release input {}", path.display()),
            error,
        )
        .into()
    })
}

/// Identifies evidence that cannot certify the requested release snapshot.
#[ohno::error]
#[display("{reason}")]
pub(crate) struct VerificationError {
    reason: String,
}
