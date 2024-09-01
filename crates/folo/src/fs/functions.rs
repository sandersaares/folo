use crate::{fs::Result, rt::yield_now};
use std::path::Path;

/// Read the contents of a file to a vector of bytes.
pub async fn read(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    yield_now().await;

    // TODO: Verify that we are on an async worker thread owned by Folo.

    todo!()
}
