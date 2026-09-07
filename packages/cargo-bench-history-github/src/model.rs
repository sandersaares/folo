use std::fmt;
use std::str::FromStr;

use ohno::AppError;

use crate::errors::{InvalidCommitShaError, InvalidInstanceError, InvalidRepositoryError};

/// A GitHub repository in `owner/name` form.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Repository {
    owner: String,
    name: String,
}

impl Repository {
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

impl FromStr for Repository {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((owner, name)) = value.split_once('/') else {
            return Err(InvalidRepositoryError::new(value).into());
        };
        let valid_part = |part: &str| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|one| one.is_ascii_alphanumeric() || matches!(one, b'.' | b'-' | b'_'))
        };
        if !valid_part(owner) || !valid_part(name) {
            return Err(InvalidRepositoryError::new(value).into());
        }
        Ok(Self {
            owner: owner.to_owned(),
            name: name.to_owned(),
        })
    }
}

impl fmt::Display for Repository {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.owner, self.name)
    }
}

/// A namespace separating independent action instances in one repository.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Instance(String);

impl Instance {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Instance {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || !value
                .bytes()
                .all(|one| one.is_ascii_alphanumeric() || matches!(one, b'.' | b'-' | b'_'))
        {
            return Err(InvalidInstanceError::new().into());
        }
        Ok(Self(value.to_owned()))
    }
}

/// A full Git commit ID used by report freshness markers.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CommitSha(String);

impl CommitSha {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for CommitSha {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        const SHA_DIGITS: usize = 40;

        if value.len() != SHA_DIGITS || !value.bytes().all(|one| one.is_ascii_hexdigit()) {
            return Err(InvalidCommitShaError::new(value).into());
        }
        Ok(Self(value.to_ascii_lowercase()))
    }
}

/// The two independent rolling issue lifecycles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IssueKind {
    Regression,
    FailureAlert,
}

impl IssueKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Regression => "regression",
            Self::FailureAlert => "failure-alert",
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(Repository: Send, Sync, Unpin, UnwindSafe, RefUnwindSafe);
    assert_impl_all!(Instance: Send, Sync, Unpin, UnwindSafe, RefUnwindSafe);
    assert_impl_all!(CommitSha: Send, Sync, Unpin, UnwindSafe, RefUnwindSafe);

    #[test]
    fn repository_requires_exactly_owner_and_name() {
        let repository = "folo-rs/folo".parse::<Repository>().unwrap();
        assert_eq!(repository.owner(), "folo-rs");
        assert_eq!(repository.name(), "folo");
        for invalid in ["folo", "/folo", "folo-rs/", "a/b/c"] {
            assert!(invalid.parse::<Repository>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn instance_is_safe_inside_an_html_comment() {
        "folo.default".parse::<Instance>().unwrap();
        for invalid in ["", "a b", "a/b", "a-->b"] {
            assert!(invalid.parse::<Instance>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn commit_sha_requires_full_hex() {
        "0123456789abcdef0123456789abcdef01234567"
            .parse::<CommitSha>()
            .unwrap();
        for invalid in ["abc", "g123456789abcdef0123456789abcdef01234567"] {
            assert!(invalid.parse::<CommitSha>().is_err(), "{invalid}");
        }
    }
}
