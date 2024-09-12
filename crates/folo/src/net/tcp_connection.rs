use core::slice;

use crate::{
    io::{self, Buffer},
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
    /// Receives the next buffer of data, with a result of `None` signaling the connection is
    /// closed and no more data can be read.
    pub async fn receive<'a>(&mut self, buffer: &mut io::Buffer<'a>) -> io::Result<Option<()>> {
        // TODO: Errr, operation() wants to own the resulting buffer. So how can we just grab one
        // to fill? Need to design proper lifetime handling here.

        // SAFETY: We are required to pass the OVERLAPPED pointer to the completion routine. We do.
        let result = unsafe {
            current_async_agent::with_io(|io| io.operation(io::Buffer::from_pool())).begin(
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
        .await?;

    // TODO: How to return result? We have a reference from CompleteBlock buuuuut...
    // Use caller buffer instead, not returning one?

        todo!()
    }

    /// Sends a buffer of data.
    pub async fn send<'b>(&mut self, buffer: io::Buffer<'b>) -> io::Result<()> {
        todo!()
    }
}

#[negative_impl]
impl !Send for TcpConnection {}
#[negative_impl]
impl !Sync for TcpConnection {}
