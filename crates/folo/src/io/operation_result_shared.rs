use crate::io::Buffer;
use crate::mem::isolation::Shared;
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
    pub buffer: Buffer<Shared>,
}

impl OperationErrorShared {
    pub fn new(inner: crate::io::Error, buffer: Buffer<Shared>) -> Self {
        Self { inner, buffer }
    }

    pub fn into_inner(self) -> crate::io::Error {
        self.inner
    }

    pub fn into_inner_and_buffer(self) -> (crate::io::Error, Buffer<Shared>) {
        (self.inner, self.buffer)
    }
}

pub type OperationResultShared = std::result::Result<Buffer<Shared>, OperationErrorShared>;

pub trait OperationResultSharedExt {
    fn into_inner(self) -> crate::io::Result<Buffer<Shared>>;
}

impl OperationResultSharedExt for OperationResultShared {
    fn into_inner(self) -> crate::io::Result<Buffer<Shared>> {
        match self {
            Ok(buffer) => Ok(buffer),
            Err(OperationErrorShared { inner, .. }) => Err(inner),
        }
    }
}
