use core::panic;
use std::ffi::CString;
use windows::{
    core::{Owned, PCSTR},
    Win32::{
        Foundation::{ERROR_IO_PENDING, GENERIC_READ, HANDLE, INVALID_HANDLE_VALUE},
        Storage::FileSystem::{
            CreateFileA, ReadFile, FILE_FLAG_OVERLAPPED, FILE_SHARE_READ, OPEN_EXISTING,
        },
        System::{
            Threading::INFINITE,
            IO::{
                CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
            },
        },
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        // Create the port - this can be used for multiple files etc. We have one port per thread,
        // regardless of what thing or how many things use it. One port provides all the completions
        // for one thread.
        let completion_port = Owned::new(CreateIoCompletionPort(
            INVALID_HANDLE_VALUE,
            HANDLE::default(),
            0,
            1,
        )?);

        let filename = CString::new("Readme.md").unwrap();

        // This just opens the file but remains fairly inert by itself.
        let file = Owned::new(CreateFileA(
            PCSTR::from_raw(filename.as_ptr() as *const u8),
            GENERIC_READ.0,
            FILE_SHARE_READ,
            None,
            OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED,
            None,
        )?);

        // Associate the file with the completion port. This informs the OS where to send IO
        // completion notifications. The return value is the same port handle, which we ignore.
        _ = CreateIoCompletionPort(*file, *completion_port, 0, 0)?;

        // Now we can start reading the file.
        let mut bytes_read: usize = 0;

        loop {
            let mut buffer = vec![0u8; 1024];

            // We can append our own custom data to the end of the OVERLAPPED structure if we need
            // to. This is a typical way to pass context to the completion handler. Here we sort of
            // cheat because there is only one thing that could possibly complete, so we "know".
            //
            // In a real implementation, both the buffer and the OVERLAPPED would likely be part of
            // a greater "operation" data structure that is self-contained and owns the parent item
            // such as the file handle, so the ongoing operations keep alive the items they are
            // operating on (in addition to callers who care about this, if any remain; it may be
            // that ongoing operations are the last remaining thing that cares about an item)
            let mut overlapped = OVERLAPPED::default();

            // We must specify what file position to start the read from.
            overlapped.Anonymous.Anonymous.Offset = bytes_read as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (bytes_read >> 32) as u32;

            // This starts the read operation. The OS takes ownership of "buffer" and "overlapped".
            // It returns with a fake error ERROR_IO_PENDING which really just means "OK".
            match ReadFile(
                *file,
                Some(buffer.as_mut_slice()),
                None,
                Some((&mut overlapped) as *mut _),
            ) {
                Ok(_) => {
                    // TODO: It is still allowed to complete synchronously, isn't it?
                    panic!("ReadFile should not return OK - we are in asynchronous mode!");
                }
                Err(e) if e.code() == ERROR_IO_PENDING.into() => {
                    // The read is pending. We must wait for it to complete.
                }
                Err(e) => {
                    panic!("ReadFile failed: {}", e);
                }
            }

            // Wait for the read to complete. This is a blocking call. In a real IO driver we would
            // poll this with a timeout T which is the time until the next scheduled timer. We would
            // also post a custom completion notification into the completion port if we need to
            // wake up for other reasons (e.g. a new task was scheduled, so we need to wake even if
            // there has been no IO).
            //
            // In our example, there will be just 1 completed item filled in here on completion
            // because all we did was enqueue one read operation.
            let mut completed_entries_buffer = vec![OVERLAPPED_ENTRY::default(); 1024];

            let mut completed_items: u32 = 0;
            GetQueuedCompletionStatusEx(
                *completion_port,
                completed_entries_buffer.as_mut_slice(),
                &mut completed_items as *mut _,
                INFINITE,
                false,
            )?;

            for index in 0..completed_items {
                // In a real app, we would cast the OVERLAPPED to our "operation" and identify the
                // real operation that just completed, so we can associate with completion with some
                // meaningful context. For example purposes, we just ignore that for now.
                let this_bytes_read =
                    completed_entries_buffer[index as usize].dwNumberOfBytesTransferred as usize;

                if this_bytes_read == 0 {
                    println!("Read 0 bytes - EOF");
                    return Ok(());
                }

                bytes_read += this_bytes_read;

                println!("Read cumulative {} bytes", bytes_read);
            }
        }
    }
}
