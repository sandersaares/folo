use crate::{
    io,
    metrics::{Event, EventBuilder},
    util::{OwnedHandle, ThreadSafe},
};
use negative_impl::negative_impl;
use std::sync::Arc;
use windows::Win32::{
    Foundation::{HANDLE, INVALID_HANDLE_VALUE},
    Storage::FileSystem::SetFileCompletionNotificationModes,
    System::{
        WindowsProgramming::{FILE_SKIP_COMPLETION_PORT_ON_SUCCESS, FILE_SKIP_SET_EVENT_ON_HANDLE},
        IO::CreateIoCompletionPort,
    },
};

// We allow the handle to be shared across threads because other threads may be sending us
// messages via the completion port. Typically, HANDLE is not thread-safe but that is merely
// because HANDLE is overly general and not all types of handles are legitimately shared across
// threads.
pub(crate) type CompletionPortHandle = Arc<ThreadSafe<OwnedHandle>>;

/// The I/O completion port is used to notify the I/O driver that an I/O operation has completed.
/// It must be associated with each file/socket/handle that is capable of asynchronous I/O. We do
/// not expose this in the public API, just use it internally to implement I/O primitives.
///
/// Each async worker thread has a single I/O completion port used for all I/O operations. This type
/// is single-threaded to prevent accidental sharing between threads.
#[derive(Debug)]
pub(crate) struct CompletionPort {
    // This is a shared handle, which means it is plausible that even after the CompletionPort
    // object is dropped, the actual completion port remains in existence. That is not ideal but
    // is largely going to be harmless (all it will do is collect wakeup notifications that nobody
    // will read). The resource leak this may produce is likely minimal, since with the thread gone,
    // there is unlikely to be much interest in any other threads waking it up again.
    handle: CompletionPortHandle,
}

impl CompletionPort {
    pub(crate) fn new() -> Self {
        // SAFETY: We wrap it in OwnedHandle, ensuring it is released when dropped. I/O completion
        // ports are safe to close from any thread, as required by the OwnedHandle API contract.
        let handle = unsafe {
            OwnedHandle::new(CreateIoCompletionPort(
                INVALID_HANDLE_VALUE,
                HANDLE::default(),
                0, // Ignored as we are not binding a handle to the port.
                1, // The port is only to be read from by one thread (the current thread).
            ).expect("creating an I/O completion port should never fail unless the OS is critically out of resources"))
        };

        // SAFETY: See comment on CompletionPortHandle.
        let handle = Arc::new(unsafe { ThreadSafe::new(handle) });

        Self { handle }
    }

    /// Binds an I/O primitive to the completion port when provided a handle to the I/O primitive.
    /// This causes notifications from that I/O primitive to arrive at the completion port.
    pub(crate) fn bind(&self, handle: &HANDLE) -> io::Result<()> {
        // SAFETY: Our handle cannot be invalid because we are keeping it alive via Arc.
        // SAFETY: We ignore the return value, because it is the same as our own handle on success.
        unsafe {
            CreateIoCompletionPort(*handle, ***self.handle, 0, 1)?;
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
                *handle,
                (FILE_SKIP_SET_EVENT_ON_HANDLE | FILE_SKIP_COMPLETION_PORT_ON_SUCCESS) as u8,
            )?;
        }

        PRIMITIVES_BOUND.with(Event::observe_unit);

        Ok(())
    }

    /// Obtains a thread-safe handle to the completion port. The primary use case is to give this
    /// to an IoWaker so that it can be used to wake up the thread that owns this completion port.
    pub(crate) fn handle(&self) -> CompletionPortHandle {
        Arc::clone(&self.handle)
    }
}

#[negative_impl]
impl !Send for CompletionPort {}
#[negative_impl]
impl !Sync for CompletionPort {}

thread_local! {
    static PRIMITIVES_BOUND: Event = EventBuilder::new()
        .name("io_primitives_bound")
        .build()
        .unwrap();
}
