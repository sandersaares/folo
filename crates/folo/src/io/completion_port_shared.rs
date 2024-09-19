use crate::{
    io::{self, IoPrimitive},
    metrics::{Event, EventBuilder},
    windows::OwnedHandle,
};
use windows::Win32::{
    Foundation::{HANDLE, INVALID_HANDLE_VALUE},
    Storage::FileSystem::SetFileCompletionNotificationModes,
    System::{
        WindowsProgramming::{FILE_SKIP_COMPLETION_PORT_ON_SUCCESS, FILE_SKIP_SET_EVENT_ON_HANDLE},
        IO::CreateIoCompletionPort,
    },
};

/// The I/O completion port is used to notify the I/O driver that an I/O operation has completed.
/// It must be associated with each file/socket/handle that is capable of asynchronous I/O. We do
/// not expose this in the public API, just use it internally to implement I/O primitives.
///
/// There are two different types of I/O completion ports in Folo:
///
/// 1. Each async worker thread has a single I/O completion port dedicated for all thread-isolated
///    I/O operations that never extend outside that thread.
/// 2. There is a shared I/O completion port used for multithreaded I/O operations. All async worker
///    threads share this and process I/O whose completion is scheduled via this port.
///
/// This is the multithreaded completion port that is shared by all async workers and used for I/O
/// that spans across threads. Just wrap it in an Arc. Interior mutability is used - no need to take
/// any exclusive references.
#[derive(Debug)]
pub(crate) struct CompletionPortShared {
    handle: OwnedHandle<HANDLE>,
}

impl CompletionPortShared {
    pub(crate) fn new() -> Self {
        // SAFETY: We wrap it in OwnedHandle, ensuring it is released when dropped. I/O completion
        // ports are safe to close from any thread, as required by the OwnedHandle API contract.
        let handle = unsafe {
            OwnedHandle::new(CreateIoCompletionPort(
                INVALID_HANDLE_VALUE,
                HANDLE::default(),
                // Ignored as we are not binding a handle to the port.
                0,
                // The port may be read by one thread per processor (which also happens to be
                // the max number of async worker threads Folo can create)
                0,
            ).expect("creating an I/O completion port should never fail unless the OS is critically out of resources"))
        };

        Self { handle }
    }

    /// Binds an I/O primitive to the completion port when provided a handle to the I/O primitive.
    /// This causes notifications from that I/O primitive to arrive at the completion port.
    pub(crate) fn bind(&self, handle: &(impl Into<IoPrimitive> + Copy)) -> io::Result<()> {
        let handle = HANDLE::from((*handle).into());

        // SAFETY: Our own handle cannot be invalid because we are keeping it alive via Arc.
        // We have to assume the user provided a valid handle (but if not, it will just be an
        // error result). We ignore the return value because it is our own handle on success.
        unsafe {
            CreateIoCompletionPort(handle, *self.handle, 0, 1)?;
        }

        // Why FILE_SKIP_SET_EVENT_ON_HANDLE: https://devblogs.microsoft.com/oldnewthing/20200221-00/?p=103466/
        //
        // SAFETY:
        // * We rely on the caller to ensure they are passing a valid I/O primitive handle.
        // * Afterwards we cannot rely on file handles being secretly treated as events. That
        //   is fine because the whole point is that we do not want to use them as events.
        // * Afterwards we will not get completion notifications for synchronous I/O operations. We
        //   must handle all synchronous completions inline. That is also fine - we do this in the
        //   PrepareBlock::begin() method, where we only use the completion port if we get a status
        //   code with ERROR_IO_PENDING.
        unsafe {
            SetFileCompletionNotificationModes(
                handle,
                (FILE_SKIP_SET_EVENT_ON_HANDLE | FILE_SKIP_COMPLETION_PORT_ON_SUCCESS) as u8,
            )?;
        }

        PRIMITIVES_BOUND.with(Event::observe_unit);

        Ok(())
    }

    /// Returns the native handle that is necessary for invoking operating system I/O APIs.
    pub(crate) fn as_native_handle(&self) -> &HANDLE {
        &self.handle
    }
}

thread_local! {
    static PRIMITIVES_BOUND: Event = EventBuilder::new("shared_io_primitives_bound")
        .build();
}
