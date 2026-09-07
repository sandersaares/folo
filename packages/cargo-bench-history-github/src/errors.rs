use std::io;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::PathBuf;

#[ohno::error]
#[display("GitHub repository must have the form `owner/name`, got '{value}'")]
pub(crate) struct InvalidRepositoryError {
    value: String,
}

impl UnwindSafe for InvalidRepositoryError {}
impl RefUnwindSafe for InvalidRepositoryError {}

#[ohno::error]
#[display("Action instance must contain only ASCII letters, digits, `.`, `-` or `_`")]
pub(crate) struct InvalidInstanceError;

impl UnwindSafe for InvalidInstanceError {}
impl RefUnwindSafe for InvalidInstanceError {}

#[ohno::error]
#[display("Commit ID must be a full 40-digit hexadecimal SHA, got '{value}'")]
pub(crate) struct InvalidCommitShaError {
    value: String,
}

impl UnwindSafe for InvalidCommitShaError {}
impl RefUnwindSafe for InvalidCommitShaError {}

#[ohno::error]
#[display("Neither `--repo` nor GITHUB_REPOSITORY identifies the repository")]
pub(crate) struct MissingRepositoryError;

impl UnwindSafe for MissingRepositoryError {}
impl RefUnwindSafe for MissingRepositoryError {}

#[ohno::error]
#[display("Neither GITHUB_TOKEN nor GH_TOKEN supplies a GitHub token")]
pub(crate) struct MissingTokenError;

impl UnwindSafe for MissingTokenError {}
impl RefUnwindSafe for MissingTokenError {}

#[ohno::error]
#[display("Failed to read report body from '{}'", path.display())]
pub(crate) struct ReadBodyError {
    path: PathBuf,
}

impl UnwindSafe for ReadBodyError {}
impl RefUnwindSafe for ReadBodyError {}

#[ohno::error]
#[display("GitHub request failed while {operation}")]
pub(crate) struct RequestFailedError {
    operation: String,
}

impl UnwindSafe for RequestFailedError {}
impl RefUnwindSafe for RequestFailedError {}

#[ohno::error]
#[display("GitHub returned HTTP {status} while {operation}: {body}")]
pub(crate) struct UnexpectedStatusError {
    operation: String,
    status: u16,
    body: String,
}

impl UnwindSafe for UnexpectedStatusError {}
impl RefUnwindSafe for UnexpectedStatusError {}

impl UnexpectedStatusError {
    pub(crate) fn status(&self) -> u16 {
        self.status
    }
}

#[ohno::error]
#[display("GitHub returned invalid JSON while {operation}")]
pub(crate) struct InvalidResponseError {
    operation: String,
}

impl UnwindSafe for InvalidResponseError {}
impl RefUnwindSafe for InvalidResponseError {}

#[ohno::error]
#[display("GitHub did not return the created artifact while {operation}")]
pub(crate) struct MissingCreatedArtifactError {
    operation: String,
}

impl UnwindSafe for MissingCreatedArtifactError {}
impl RefUnwindSafe for MissingCreatedArtifactError {}

#[ohno::error]
#[display("GitHub create remained ambiguous after marker reconciliation")]
pub(crate) struct AmbiguousCreateError;

impl UnwindSafe for AmbiguousCreateError {}
impl RefUnwindSafe for AmbiguousCreateError {}

pub(crate) fn read_body_error(path: PathBuf, error: io::Error) -> ohno::AppError {
    ReadBodyError::caused_by(path, error).into()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn unexpected_status_exposes_its_status_for_delete_reconciliation() {
        let not_found = reqwest::StatusCode::NOT_FOUND.as_u16();
        let error = UnexpectedStatusError::new("deleting", not_found, "missing");
        assert_eq!(error.status(), not_found);
    }
}
