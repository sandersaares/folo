use crate::{
    io::{self, Buffer},
    rt::{current_async_agent, spawn_blocking},
    util::SendHandle,
};
use std::{ffi::CString, ops::ControlFlow, path::Path};
use tracing::{event, Level};
use windows::{
    core::{Owned, PCSTR},
    Win32::{
        Foundation::{HANDLE, STATUS_END_OF_FILE},
        Storage::FileSystem::{
            CreateFileA, GetFileSizeEx, ReadFile, FILE_FLAG_OVERLAPPED, FILE_FLAG_SEQUENTIAL_SCAN,
            FILE_GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING,
        },
    },
};

// TODO: Review https://devblogs.microsoft.com/oldnewthing/20220425-00/?p=106526 for some good testing advice.

pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    read_large_buffer(path).await
}

/// Read the contents of a file to a vector of bytes using small pooled buffers.
pub async fn read_small_buffer(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    event!(
        Level::INFO,
        message = "read()",
        path = path.as_ref().display().to_string()
    );

    let path_cstr = CString::new(path.as_ref().to_str().unwrap()).unwrap();

    unsafe {
        // Opening the file and probing its size are blocking operations, so we kick them off to
        // a synchronous worker thread to avoid blocking the async workers with these slow calls.

        let (file_handle, file_size) = spawn_blocking(move || -> io::Result<_> {
            let file_handle = Owned::new(CreateFileA(
                PCSTR::from_raw(path_cstr.as_ptr() as *const u8),
                FILE_GENERIC_READ.0,
                FILE_SHARE_READ,
                None,
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
                None,
            )?);

            // Get the size first to allocate the buffer with the correct size. If the size changes
            // while we read it, that is fine - this is just the initial allocation and may change.
            let mut file_size: i64 = 0;

            GetFileSizeEx(*file_handle, &mut file_size as *mut _)?;

            event!(Level::DEBUG, message = "opened file", file_size);

            let file_handle = SendHandle::from(*file_handle);
            Ok((file_handle, file_size))
        })
        .await?;

        // For transport across threads, we need to pack the HANDLE into a SendHandle but now we
        // can again declare ownership via Owned<HANDLE>.
        let file_handle = Owned::new(file_handle.into());

        current_async_agent::with_io(|io| io.bind_io_primitive(&file_handle))?;

        let mut result = Vec::with_capacity(file_size as usize);

        while read_bytes_from_file(&file_handle, result.len(), &mut result).await?
            == ControlFlow::Continue(())
        {}

        event!(
            Level::TRACE,
            message = "read() complete",
            length = result.len()
        );

        Ok(result)
    }
}

/// Read the contents of a file to a vector of bytes using one giant buffer for the entire file.
pub async fn read_large_buffer(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
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

        current_async_agent::with_io(|io| io.bind_io_primitive(&file))?;

        // Get the size first to allocate the buffer with the correct size. If the size changes
        // while we read it, that is fine - this is just the initial allocation and may change.
        let mut file_size: i64 = 0;

        // TODO: inspecting file metadata a blocking function and potentially slow!
        // Perhaps better to examine metadata on sync worker to avoid latency spikes here?
        GetFileSizeEx(*file, &mut file_size as *mut _)?;

        // We create a vector with what we think is the correct capacity and zero-initialize it.
        let mut result = Vec::with_capacity(file_size as usize);
        result.resize(result.capacity(), 0);

        let mut bytes_read = 0;

        // This does not account for the fact that the file size may theoretically change during
        // the read operation. Not super interesting for our purposes - file is constant during
        // benchmarking.
        loop {
            // We ask the OS to read the entire file. It is within its rights to give us only a
            // part of what we asked for, so we need to be prepared to loop no matter what.
            let buffer = Buffer::from_slice(&mut result[bytes_read..]);

            match read_buffer_from_file(&file, bytes_read, buffer).await? {
                ControlFlow::Continue(bytes) => {
                    bytes_read += bytes;

                    if bytes_read == result.len() {
                        // We have read the entire file (we think). We are done.
                        break;
                    }
                }
                ControlFlow::Break(()) => {
                    // All done! L
                    assert_eq!(bytes_read, result.len());
                    break;
                }
            }
        }

        event!(
            Level::TRACE,
            message = "read() complete",
            length = result.len()
        );

        Ok(result)
    }
}

/// Reads a chunk of bytes from a file at a given offset using a small intermediate buffer.
async fn read_bytes_from_file(
    file: &Owned<HANDLE>,
    offset: usize,
    dest: &mut Vec<u8>,
) -> io::Result<ControlFlow<()>> {
    let mut operation = current_async_agent::with_io(|io| io.operation(io::Buffer::from_pool()));
    operation.set_offset(offset);

    // SAFETY: For safe usage of the I/O driver API, we are required to pass the `overlapped`
    // argument to a native I/O call under all circumstances, to trigger an I/O completion. We do.
    // We are also not allowed to use any of the callback arguments after the callback, even if
    // the Rust compiler might allow us to.
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

/// Reads a chunk of bytes from a file at a given offset and fills the provided buffer with them.
/// This is a zero-copy read mechanism - data goes straight into the final buffer you provide.
///
/// Returns the number of bytes read into the buffer on success.
async fn read_buffer_from_file(
    file: &Owned<HANDLE>,
    offset: usize,
    buffer: Buffer<'_>,
) -> io::Result<ControlFlow<(), usize>> {
    let mut operation = current_async_agent::with_io(|io| io.operation(buffer));
    operation.set_offset(offset);

    // SAFETY: For safe usage of the I/O driver API, we are required to pass the `overlapped`
    // argument to a native I/O call under all circumstances, to trigger an I/O completion. We do.
    // We are also not allowed to use any of the callback arguments after the callback, even if
    // the Rust compiler might allow us to.
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
        Ok(result) => Ok(ControlFlow::Continue(result.buffer().len())),
        Err(io::Error::External(e)) if e.code() == STATUS_END_OF_FILE.into() => {
            Ok(ControlFlow::Break(()))
        }
        Err(e) => Err(e),
    }
}
