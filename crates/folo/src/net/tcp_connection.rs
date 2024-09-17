use std::sync::Arc;

use crate::{
    io::{self, OperationResult, OperationResultExt, PinnedBuffer},
    net::winsock,
    rt::{current_async_agent, current_runtime, SynchronousTaskType},
    util::OwnedHandle,
};
use negative_impl::negative_impl;
use windows::{
    core::PSTR,
    Win32::Networking::WinSock::{WSARecv, WSASend, WSASendDisconnect, SOCKET, WSABUF},
};

pub struct TcpConnection {
    // This is an Arc because some operations (e.g. shutdown) involve synchronous logic and
    // therefore we must share the socket between multiple threads.
    pub(super) socket: Arc<OwnedHandle<SOCKET>>,
}

impl TcpConnection {
    /// Receives the next buffer of data.
    ///
    /// The buffer will be returned in the result with the active region set to the bytes read, with
    /// a length of 0 if the connection was closed.
    ///
    /// You should not call this multiple times concurrently because there is no guarantee that the
    /// continuations will be called in a particular order.
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
                        **self.socket,
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

    /// Sends a buffer of data to the peer.
    ///
    /// The buffer will be returned in the result to allow reuse.
    ///
    /// You may call this multiple times concurrently. The buffers will be sent in the order they
    /// are submitted.
    pub async fn send(&mut self, buffer: PinnedBuffer) -> OperationResult {
        // SAFETY: We are required to pass the OVERLAPPED pointer to the completion routine. We do.
        unsafe {
            current_async_agent::with_io(|io| io.new_operation(buffer)).begin(
                |buffer, overlapped, immediate_bytes_transferred| {
                    let wsabuf = WSABUF {
                        len: buffer.len() as u32,
                        buf: PSTR::from_raw(buffer.as_mut_ptr()),
                    };

                    let wsabufs = [wsabuf];

                    winsock::to_io_result(WSASend(
                        **self.socket,
                        &wsabufs,
                        Some(immediate_bytes_transferred as *mut u32),
                        0,
                        Some(overlapped),
                        None,
                    ))
                },
            )
        }
        .await
    }

    /// Performs a graceful shutdown of the connection, allowing time for all pending data transfers
    /// to complete. After this, you may drop the object and be assured that no data was lost in
    /// transit - this guarantee does not exist without calling the shutdown method.
    ///
    /// An error result indicates that a graceful shutdown was not possible.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        // How this works is that we:
        // 1) Tell the OS that we will not send any more data. This sets the wheels in motion.
        // 2) Try to read more data from the peer and expect to see a 0-byte result (EOF).
        //    Actually getting some data here is an error - this indicates that the caller did not
        //    fully process incoming data before calling shutdown. Potentially valid for some types
        //    of connections (e.g. an infinite stream of values) but not typical for web APIs.
        // 3) Done! Once we get the EOF, we can be sure that the peer has received all of our data
        //    and our FIN has been acknowledged, so no more activity can occur on the wire

        let socket_clone = Arc::clone(&self.socket);
        current_runtime::with(|runtime| {
            runtime.spawn_sync(SynchronousTaskType::Syscall, move || {
                // SAFETY: Socket liveness is ensured by our shared ownership of the socket handle.
                winsock::to_io_result(unsafe { WSASendDisconnect(**socket_clone, None) })
            })
        })
        .await?;

        let received_data = self.receive(PinnedBuffer::from_pool()).await.into_inner()?;

        if !received_data.is_empty() {
            return Err(io::Error::LogicError("socket received data when shutting down - this may be an error depending on the communication protocol in use".to_string()));
        }

        Ok(())
    }
}

#[negative_impl]
impl !Send for TcpConnection {}
#[negative_impl]
impl !Sync for TcpConnection {}
