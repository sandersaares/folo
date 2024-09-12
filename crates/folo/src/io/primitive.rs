use windows::Win32::{Foundation::HANDLE, Networking::WinSock::SOCKET};

/// Windows I/O primitives use the same handles underneath, even if exposed via different types.
/// Different APIs expect one or another type. This allows us to accept any compatible type.
pub struct IoPrimitive {
    raw: *mut core::ffi::c_void,
}

impl From<HANDLE> for IoPrimitive {
    fn from(handle: HANDLE) -> Self {
        Self {
            raw: handle.0 as *mut _,
        }
    }
}

impl From<SOCKET> for IoPrimitive {
    fn from(socket: SOCKET) -> Self {
        Self {
            raw: socket.0 as *mut _,
        }
    }
}

impl From<IoPrimitive> for HANDLE {
    fn from(primitive: IoPrimitive) -> Self {
        HANDLE(primitive.raw)
    }
}

impl From<IoPrimitive> for SOCKET {
    fn from(primitive: IoPrimitive) -> Self {
        SOCKET(primitive.raw as usize)
    }
}
