use crate::io::PinnedBufferShared;
use thiserror::Error;

/// An error for an I/O operation that was attempted on a data buffer. Contains not only the error
/// information but also the data buffer that was used, enabling it to be inspected or reused.
///
/// If you do not care about the buffer, simply call `into_inner()` to extract the inner error.
///
/// This is used for multithreaded I/O operations where execution of the operation is shared
/// across any number of threads.
#[derive(Debug, Error)]
#[error("shared I/O operation failed: {inner}")]
pub struct OperationErrorShared {
    pub inner: crate::io::Error,
    pub buffer: PinnedBufferShared,
}

impl OperationErrorShared {
    pub fn new(inner: crate::io::Error, buffer: PinnedBufferShared) -> Self {
        Self { inner, buffer }
    }

    pub fn into_inner(self) -> crate::io::Error {
        self.inner
    }

    pub fn into_inner_and_buffer(self) -> (crate::io::Error, PinnedBufferShared) {
        (self.inner, self.buffer)
    }
}

pub type OperationResultShared = std::result::Result<PinnedBufferShared, OperationErrorShared>;

pub trait OperationResultSharedExt {
    fn into_inner(self) -> crate::io::Result<PinnedBufferShared>;
}

impl OperationResultSharedExt for OperationResultShared {
    fn into_inner(self) -> crate::io::Result<PinnedBufferShared> {
        match self {
            Ok(buffer) => Ok(buffer),
            Err(OperationErrorShared { inner, .. }) => Err(inner),
        }
    }
}
