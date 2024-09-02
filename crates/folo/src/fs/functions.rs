use std::{ffi::CString, path::Path};
use windows::{
    core::{Owned, PCSTR},
    Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{
            CreateFileA, ReadFile, FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_SHARE_READ,
            OPEN_EXISTING,
        },
    },
};

use crate::{io, rt::current_agent};

/// Read the contents of a file to a vector of bytes.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
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
            FILE_FLAG_OVERLAPPED,
            None,
        )?);

        // TODO: Associate file handle with completion port.

        let mut result = Vec::new();

        while let Some(bytes) = read_bytes_from_file(&file, result.len()).await? {
            result.extend_from_slice(&bytes);
        }

        Ok(result)
    }

    // TODO: Optimize this. Some ideas:
    // * Avoid allocating new buffers all over the place.
    // * Do not make each ReadFile a task, rather follow them up directly in IO completion handler.
    // * Read data directly into output vector and shrink to fit, if there is some API for that.
    // * Handle immediate completions inline.

    // TODO: Review https://devblogs.microsoft.com/oldnewthing/20220425-00/?p=106526
}

async fn read_bytes_from_file(file: &Owned<HANDLE>, offset: usize) -> io::Result<Option<Vec<u8>>> {
    unsafe {
        let io = current_agent::get().io().borrow_mut().begin_io();
        let operation_result = io
            .from_offset(offset)
            .apply(|buffer, overlapped| Ok(ReadFile(**file, Some(buffer), None, Some(overlapped))?))
            .await?;

        if operation_result.payload().is_empty() {
            return Ok(None);
        }

        Ok(Some(operation_result.payload().to_vec()))
    }
}
