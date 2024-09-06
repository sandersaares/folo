use windows::Win32::Foundation::HANDLE;

/// Forcibly treats a HANDLE as thread-safe, which they normally are not due to Rust type system
/// defaults (even though in a Win32 context HANDLE is generally safe, although it might depend
/// on the exact type of handle).
pub struct SendHandle {
    handle: HANDLE,
}

unsafe impl Send for SendHandle {}

impl From<HANDLE> for SendHandle {
    fn from(handle: HANDLE) -> Self {
        Self { handle }
    }
}

impl From<SendHandle> for HANDLE {
    fn from(send_handle: SendHandle) -> HANDLE {
        send_handle.handle
    }
}
