use crate::io;
use std::sync::LazyLock;
use windows::Win32::Networking::WinSock::{WSAGetLastError, WSAStartup, WSADATA};

pub fn ensure_initialized() {
    *WINSOCK_STARTUP;
}

static WINSOCK_STARTUP: LazyLock<()> = LazyLock::new(|| {
    let mut data = WSADATA::default();
    // SAFETY: We are passing a valid pointer, which is fine. While Winsock does require us to
    // cleanup afterwards, we intentionally do not do this - we expect to keep Winsock active
    // for the entire life of the process. If it fails, we panic and expect the process to die.
    assert_eq!(0, unsafe { WSAStartup(0x202, &mut data as *mut _) });
});

/// Converts a Winsock result into an IO result.
pub fn to_io_result(winsock_result: i32) -> io::Result<()> {
    if winsock_result == 0 {
        Ok(())
    } else {
        // SAFETY: Nothing unsafe here, just an FFI call.
        let specific_error = unsafe { WSAGetLastError() };

        Err(io::Error::Winsock {
            code: winsock_result,
            detail: specific_error,
        })
    }
}
