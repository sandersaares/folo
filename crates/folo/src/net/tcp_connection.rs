use crate::{
    io::{OperationResult, PinnedBuffer},
    net::winsock,
    rt::current_async_agent,
    util::OwnedHandle,
};
use negative_impl::negative_impl;
use windows::{
    core::PSTR,
    Win32::Networking::WinSock::{WSARecv, SOCKET, WSABUF},
};

pub struct TcpConnection {
    pub(super) socket: OwnedHandle<SOCKET>,
}

impl TcpConnection {
    /// Receives the next buffer of data. The buffer will be returned with the active region set to
    /// the bytes read, with a length of 0 if the connection was closed.
    pub async fn receive(&mut self, buffer: PinnedBuffer) -> OperationResult {
        // SAFETY: We are required to pass the OVERLAPPED pointer to the completion routine. We do.
        unsafe {
            current_async_agent::with_io(|io| io.new_operation(buffer)).begin(
                |buffer, overlapped, immediate_bytes_transferred| {
                    let wsabuf = WSABUF {
                        len: buffer.len() as u32,
                        buf: PSTR::from_raw(buffer.as_mut_ptr()),
                    };

                    let wsabufs = [wsabuf];
                    let mut flags: u32 = 0;

                    winsock::to_io_result(WSARecv(
                        *self.socket,
                        &wsabufs,
                        Some(immediate_bytes_transferred as *mut u32),
                        &mut flags as *mut u32,
                        Some(overlapped),
                        None,
                    ))
                },
            )
        }
        .await
    }

    /// Sends a buffer of data.
    pub async fn send(&mut self, buffer: PinnedBuffer) -> OperationResult {
        todo!()
    }
}

#[negative_impl]
impl !Send for TcpConnection {}
#[negative_impl]
impl !Sync for TcpConnection {}
