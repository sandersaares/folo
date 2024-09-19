// Copyright (c) Microsoft Corporation.

use std::borrow::Cow;
use std::time::SystemTimeError;

/// The result for fallible operations that use the [`Error`] type in the `time` module.
pub type Result<T> = std::result::Result<T, Error>;

/// An error that can occur in the `time` module.
///
/// The most common type of error is a result of overflow. But other errors
/// exist as well:
///
/// * Parsing and formatting errors for [`Timestamp`][`super::Timestamp`].
/// * Validation problems.
///
/// # Introspection is limited
///
/// Other than implementing the [`std::error::Error`] and [`core::fmt::Debug`] trait, this error type
/// currently provides no introspection capabilities.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use folo::time::{Clock, Error, Timestamp};
///
/// let clock = Clock::new();
/// let timestamp = clock.now();
///
/// timestamp.checked_add(Duration::MAX).unwrap_err();
/// ```

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] ErrorKind);

#[derive(Debug, thiserror::Error)]
pub(super) enum ErrorKind {
    #[error(transparent)]
    SystemTime(#[from] SystemTimeError),

    #[error("{0}")]
    OutOfRange(Cow<'static, str>),
}

impl Error {
    pub(super) fn from_kind(kind: ErrorKind) -> Self {
        Self(kind)
    }

    pub(super) fn out_of_range(message: impl Into<Cow<'static, str>>) -> Self {
        Self::from_kind(ErrorKind::OutOfRange(message.into()))
    }

    #[cfg(test)]
    pub(super) fn kind(&self) -> &ErrorKind {
        &self.0
    }
}

impl From<SystemTimeError> for Error {
    fn from(error: SystemTimeError) -> Self {
        Self::from_kind(ErrorKind::SystemTime(error))
    }
}
