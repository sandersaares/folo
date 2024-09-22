use crate::{
    io::{self, Buffer},
    mem::isolation::Isolated,
    rt::{current_async_agent, spawn_sync, SynchronousTaskType},
    windows::OwnedHandle,
};
use std::{ffi::CString, path::Path, rc::Rc};
use windows::{
    core::PCSTR,
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

// Maximum size of a single read submitted to the OS. We repeat reads of up to this size until we
// have read the entire file. This is a complicated tradeoff between different factors but
// approximately speaking, a larger buffer means more time spent in ReadFile() which is somewhat bad
// as it happens on an async worker thread, but may mean fewer syscalls and less tasking chatter.
const MAX_READ_SIZE_BYTES: usize = 10 * 1024 * 1024;

/// Read the contents of a file to a vector of bytes using one giant buffer for the entire file.
pub async fn read_large_buffer(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path_cstr = CString::new(path.as_ref().to_str().unwrap()).unwrap();

    unsafe {
        // Opening the file and probing its size are blocking operations, so we kick them off to
        // a synchronous worker thread to avoid blocking the async workers with these slow calls.

        let (file_handle, file_size) =
            spawn_sync(SynchronousTaskType::Syscall, move || -> io::Result<_> {
                let file_handle = OwnedHandle::new(CreateFileA(
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

                Ok((file_handle, file_size))
            })
            .await?;

        // Now that we have it on our async worker thread, any further activities can use Rc
        // to share the lifetime between tasks because we know they will not leave this thread.
        let file_handle = Rc::new(file_handle);

        current_async_agent::with_io(|io| io.bind_io_primitive(&**file_handle))?;

        // We create a boxed slice of the correct size to use as the target of the read operation.
        // We must use a boxed slice because we need to pass ownership of the buffer to the I/O
        // driver for the duration of the I/O operation, so it cannot be rooted in the stack, nor
        // can we provide a reference while retaining ownership.
        let mut buffer = Vec::<u8>::with_capacity(file_size as usize);
        #[allow(clippy::uninit_vec)] // They are just bytes destined for overwriting, meaningless.
        buffer.set_len(buffer.capacity());
        let mut buffer = Buffer::<Isolated>::from_boxed_slice(buffer.into_boxed_slice());

        let mut bytes_read = 0;

        // This does not account for the fact that the file size may theoretically change during
        // the read operation. Not super interesting for our purposes - file is constant during
        // benchmarking and this would just mean some reallocation/copying dynamically which we
        // would in practice never run into.
        loop {
            // We ask the OS to read the entire file. It is within its rights to give us only a
            // part of what we asked for, so we need to be prepared to loop no matter what.
            buffer = read_buffer_from_file(Rc::clone(&file_handle), bytes_read, buffer).await?;
            bytes_read += buffer.len();

            if buffer.is_empty() {
                // We have read the entire file (we think). We are done.
                assert_eq!(
                    bytes_read, file_size as usize,
                    "file size changed during read"
                );

                buffer = buffer.use_all_until_current();
                let active_region = buffer.active_range();
                let inner_slice = buffer
                    .try_into_inner_boxed_slice()
                    .expect("we provided a boxed-slice buffer, so the inverse must be possible");
                let mut as_vec = inner_slice.into_vec();
                as_vec.set_len(active_region.len());
                return Ok(as_vec);
            } else {
                // More remains to be read.
                buffer = buffer.use_remainder();
            }
        }
    }
}

/// Reads a chunk of bytes from a file at a given offset and fills the provided buffer with them,
/// appending the bytes to the beginning of the buffer's active region (without changing the
/// region).
///
/// Returns the buffer in every case, with the action region of the buffer set to the data read.
/// A zero-sized active region indicates end of file.
async fn read_buffer_from_file(
    file_handle: Rc<OwnedHandle<HANDLE>>,
    offset: usize,
    mut buffer: Buffer<Isolated>,
) -> io::Result<Buffer<Isolated>> {
    if buffer.len() > MAX_READ_SIZE_BYTES {
        buffer.set_len(MAX_READ_SIZE_BYTES);
    }

    let mut operation = current_async_agent::with_io(|io| io.new_operation(buffer));
    operation.set_offset(offset);

    // SAFETY: For safe usage of the I/O driver API, we are required to pass the `overlapped`
    // argument to a native I/O call under all circumstances, to trigger an I/O completion. We do.
    // We are also not allowed to use any of the callback arguments after the callback, even if
    // the Rust compiler might allow us to.
    match unsafe {
        operation
            .begin(move |buffer, overlapped, bytes_transferred_immediately| {
                Ok(ReadFile(
                    **file_handle,
                    Some(buffer),
                    Some(bytes_transferred_immediately as *mut _),
                    Some(overlapped),
                )?)
            })
            .await
    } {
        Ok(buffer) => Ok(buffer),
        Err(io::OperationError {
            inner: io::Error::Windows(external),
            buffer,
        }) if external.code() == STATUS_END_OF_FILE.into() => Ok(buffer),
        Err(e) => Err(e.into_inner()),
    }
}
