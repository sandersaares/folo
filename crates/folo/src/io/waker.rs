use crate::io::CompletionPortHandle;
use windows::Win32::System::IO::PostQueuedCompletionStatus;

// Value is meaningless, just has to be unique.
pub(crate) const WAKE_UP_COMPLETION_KEY: usize = 0x23546789897;

/// A cross-thread element that can be used to wake up an I/O driver from another thread.
///
/// The waker itself is a "client" of sorts that can be handed over to any thread. It has a handle
/// to the completion port of the I/O driver. When it wants to wake up the I/O driver, it must post
/// a specific completion packet to the completion port.
///
/// The completion packet is simply a completion message without any payload and the completion key
/// `WAKE_UP_COMPLETION_KEY`. The OVERLAPPED pointer is null for these messages.
#[derive(Clone, Debug)]
pub(crate) struct IoWaker {
    completion_port: CompletionPortHandle,
}

impl IoWaker {
    pub(crate) fn new(completion_port: CompletionPortHandle) -> Self {
        Self { completion_port }
    }

    /// Wakes up the target thread via the I/O driver by sending a completion packet to its
    /// completion port. This is a non-blocking operation.
    pub(crate) fn wake(&self) {
        // SAFETY: Nothing to worry about - we keep our handle alive via Arc, so it must be valid.
        unsafe {
            // Note that OVERLAPPED is null here - we do not need to provide one for this operation
            // because only real operations require it - plain notifications do not.
            //
            // We ignore the result from this because it does not really matter - if anything goes
            // wrong, the target thread fails to wake up and that's too bad but nothing for us to
            // worry about - probably the entire app is going away if that happened anyway.
            _ = PostQueuedCompletionStatus(
                ***self.completion_port,
                0,
                WAKE_UP_COMPLETION_KEY,
                None,
            );
        }
    }
}
