use crate::{io, rt::current_agent};
use std::{ffi::CString, ops::ControlFlow, path::Path};
use tracing::{event, Level};
use windows::{
    core::{Owned, PCSTR},
    Win32::{
        Foundation::{HANDLE, STATUS_END_OF_FILE},
        Storage::FileSystem::{
            CreateFileA, ReadFile, FILE_FLAG_OVERLAPPED, FILE_FLAG_SEQUENTIAL_SCAN,
            FILE_GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING,
        },
    },
};

/// Read the contents of a file to a vector of bytes.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    event!(
        Level::INFO,
        message = "read()",
        path = path.as_ref().display().to_string()
    );

    let path_cstr = CString::new(path.as_ref().to_str().unwrap()).unwrap();

    unsafe {
        // TODO: opening a file is a blocking function and potentially slow!
        // Perhaps better to open the file on sync worker to avoid latency spikes here?
        let file = Owned::new(CreateFileA(
            PCSTR::from_raw(path_cstr.as_ptr() as *const u8),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ,
            None,
            OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
            None,
        )?);

        event!(Level::DEBUG, message = "opened file",);

        current_agent::with_io(|io| io.bind_io_primitive(&file))?;

        let mut result = Vec::new();

        while read_bytes_from_file(&file, result.len(), &mut result).await?
            == ControlFlow::Continue(())
        {}

        event!(
            Level::TRACE,
            message = "read() complete",
            length = result.len()
        );

        Ok(result)
    }

    // TODO: Optimize this. Some ideas:
    // * Avoid allocating new buffers all over the place.
    // * Do not make each ReadFile a task, rather follow them up directly in IO completion handler.
    // * Read data directly into output vector and shrink to fit, if there is some API for that.
    // * Handle immediate completions inline.

    // TODO: Review https://devblogs.microsoft.com/oldnewthing/20220425-00/?p=106526
}

/// Reads a chunk of bytes from a file at a given offset.
async fn read_bytes_from_file(
    file: &Owned<HANDLE>,
    offset: usize,
    dest: &mut Vec<u8>,
) -> io::Result<ControlFlow<()>> {
    let mut operation = current_agent::with_io(|io| io.operation());
    operation.set_offset(offset);

    // SAFETY: For safe usage of the I/O driver API, we are required to pass the `overlapped`
    // argument to a native I/O call under all circumstances, to trigger an I/O completion. We do.
    let result = unsafe {
        operation
            .begin(|buffer, overlapped, bytes_transferred_immediately| {
                Ok(ReadFile(
                    **file,
                    Some(buffer),
                    Some(bytes_transferred_immediately as *mut _),
                    Some(overlapped),
                )?)
            })
            .await
    };

    match result {
        // The errors here may come from the ReadFile call, or from the IO completion handler.
        // We coalesce errors from both into the single result that we see here.
        Ok(result) if result.buffer().is_empty() => Ok(ControlFlow::Break(())),
        Ok(result) => {
            dest.extend_from_slice(result.buffer());
            Ok(ControlFlow::Continue(()))
        }
        Err(io::Error::External(e)) if e.code() == STATUS_END_OF_FILE.into() => {
            Ok(ControlFlow::Break(()))
        }
        Err(e) => Err(e),
    }
}
