use crate::{
    io,
    net::{winsock, TcpConnection},
    rt::{current_async_agent, spawn},
    util::OwnedHandle,
};
use core::slice;
use negative_impl::negative_impl;
use std::{future::Future, mem};
use tracing::{event, Level};
use windows::Win32::Networking::WinSock::{
    bind, htons, listen, setsockopt, AcceptEx, GetAcceptExSockaddrs, WSASocketA, AF_INET,
    INADDR_ANY, IN_ADDR, IPPROTO_TCP, SOCKADDR, SOCKADDR_IN, SOCKET, SOCK_STREAM, SOL_SOCKET,
    SOMAXCONN, SO_UPDATE_ACCEPT_CONTEXT, WSA_FLAG_OVERLAPPED,
};

pub struct TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    port: Option<u16>,
    on_accept: Option<A>,
}

impl<A, AF> TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    pub fn new() -> Self {
        Self {
            port: None,
            on_accept: None,
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the function to call when a new connection is accepted. The function may be called
    /// from any async task worker thread and any number of times concurrently.
    ///
    /// The connection will be closed when the provided TcpConnection is dropped.
    pub fn on_accept(mut self, callback: A) -> Self {
        self.on_accept = Some(callback);
        self
    }

    /// Builds the TCP server and starts accepting new connections.
    ///
    /// The startup process is gradual and connections may be received even before the result of
    /// this function is returned. Connections may even be received if this function ultimately
    /// returns an error (though an error response does imply that no further connections will be
    /// accepted and the server has shut down after a failed start).
    pub fn build(self) -> io::Result<TcpServerHandle> {
        let port = self
            .port
            .ok_or_else(|| io::Error::InvalidOptions("port must be set".to_string()))?;
        let on_accept = self
            .on_accept
            .ok_or_else(|| io::Error::InvalidOptions("on_accept must be set".to_string()))?;

        winsock::ensure_initialized();

        // NOTE: Creating a socket is a synchronous operation, kick this to a sync worker thread.

        // TODO: Broadcast this and await the results.

        // SAFETY: We are required to close the handle once we are done with it,
        // which we do via OwnedHandle that closes the handle on drop.
        let listen_socket = unsafe {
            OwnedHandle::new(WSASocketA(
                AF_INET.0 as i32,
                SOCK_STREAM.0 as i32,
                IPPROTO_TCP.0 as i32,
                None,
                0,
                WSA_FLAG_OVERLAPPED,
            )?)
        };

        // TODO: Set send/receiver buffer sizes.
        // TODO: Set "reuse address".
        // TODO: Set SIO_SET_PORT_SHARING_PER_PROC_SOCKET.

        let mut addr = IN_ADDR::default();
        addr.S_un.S_addr = INADDR_ANY;

        let socket_addr = SOCKADDR_IN {
            sin_family: AF_INET,
            // SAFETY: Nothing unsafe here, just an FFI call.
            sin_port: unsafe { htons(port) },
            sin_addr: addr,
            sin_zero: [0; 8],
        };

        // SAFETY: All we need to be concerned about is passing in valid arguments, which we do.
        unsafe {
            winsock::to_io_result(bind(
                *listen_socket,
                &socket_addr as *const _ as *const _,
                mem::size_of::<SOCKADDR_IN>() as i32,
            ))?;

            winsock::to_io_result(listen(*listen_socket, SOMAXCONN as i32))?;
        };

        // Bind the socket to the I/O completion port so we can process I/O completions.
        current_async_agent::with_io(|io| {
            io.bind_io_primitive(&*listen_socket).unwrap();
        });

        // Now the socket is ready and we can start accepting traffic on it. We do this in a task
        // dedicated to the job of accepting new connections.
        _ = spawn(accept_connections(listen_socket, on_accept));

        Ok(TcpServerHandle {})
    }
}

#[negative_impl]
impl<A, AF> !Send for TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}
#[negative_impl]
impl<A, AF> !Sync for TcpServerBuilder<A, AF>
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
}

async fn accept_connections<A, AF>(listen_socket: OwnedHandle<SOCKET>, on_accept: A)
where
    A: Fn(TcpConnection) -> AF + Clone + Send + 'static,
    AF: Future<Output = io::Result<()>> + 'static,
{
    // TODO: How to handle errors? Presumably we should ignore them but emit telemetry.
    // Perhaps a callback to send any errors to and let the caller worry about it?

    // TODO: Should we enqueue multiple accept() in parallel or is one enough?

    // For now, we will loop forever. Later, we might implement some shutdown signal.
    loop {
        // SAFETY: All we need to worry about here is cleanup, which we do via OwnedHandle.
        let connection_socket = unsafe {
            OwnedHandle::new(
                WSASocketA(
                    AF_INET.0 as i32,
                    SOCK_STREAM.0 as i32,
                    IPPROTO_TCP.0 as i32,
                    None,
                    0,
                    WSA_FLAG_OVERLAPPED,
                )
                .expect("creating connection socket failed"),
            )
        };

        // NOTE: AcceptEx supports immediately pasting the first block of received data in here,
        // which may provide a performance boost when accepting the connection. This is optional
        // and for now we disable this via setting dwReceiveDataLength to 0.
        //
        // Contents (not in order):
        // * Local address
        // * Remote address
        // * (Optional) first block of data received
        //
        // Reference of relevant length calculations:
        // bRetVal = lpfnAcceptEx(ListenSocket, AcceptSocket, lpOutputBuf,
        //      outBufLen - ((sizeof (sockaddr_in) + 16) * 2),
        //      sizeof (sockaddr_in) + 16, sizeof (sockaddr_in) + 16,
        //      &dwBytes, &olOverlap);
        let buffer = io::Buffer::from_pool();

        // The data length in the buffer (if we were to want to use some) would be the buffer size
        // minus double of this (local + remote address).
        const ADDRESS_LENGTH: usize = mem::size_of::<SOCKADDR_IN>() + 16;

        assert!(buffer.len() >= ADDRESS_LENGTH * 2);

        let operation = current_async_agent::with_io(|io| {
            io.bind_io_primitive(&*connection_socket).unwrap();
            io.operation(buffer)
        });

        // SAFETY: We are required to pass the OVERLAPPED struct to the native I/O function to avoid
        // a resource leak. We do.
        let completed = unsafe {
            operation.begin(|buffer, overlapped, immediate_bytes_transferred| {
                if AcceptEx(
                    *listen_socket,
                    *connection_socket,
                    buffer.as_mut_ptr() as *mut _,
                    0,
                    ADDRESS_LENGTH as u32,
                    ADDRESS_LENGTH as u32,
                    immediate_bytes_transferred,
                    overlapped,
                )
                .as_bool()
                {
                    Ok(())
                } else {
                    // The docs say it sets ERROR_IO_PENDING in WSAGetLastError. We do not strictly
                    // speaking read that but it also seems to set ERROR_IO_PENDING in the regular
                    // GetLastError, apparently, so all is well.
                    Err(windows::core::Error::from_win32().into())
                }
            })
        }
        .await
        .unwrap();

        event!(Level::INFO, "AcceptEx completed");

        let payload = completed.buffer();

        let mut local_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut local_addr_len: i32 = 0;
        let mut remote_addr: *mut SOCKADDR = std::ptr::null_mut();
        let mut remote_addr_len: i32 = 0;

        // SAFETY: As long as we pass in valid pointers that match the AcceptEx call, we are good.
        unsafe {
            GetAcceptExSockaddrs(
                payload.as_ptr() as *const _,
                0,
                ADDRESS_LENGTH as u32,
                ADDRESS_LENGTH as u32,
                &mut local_addr as *mut _,
                &mut local_addr_len as *mut _,
                &mut remote_addr as *mut _,
                &mut remote_addr_len as *mut _,
            )
        };

        // We need to refer to this via pointer, so let's copy it out to an lvalue first.
        let listen_socket = listen_socket.0;
        // SAFETY: The size is right, so creation the slice is OK. We only use it for the single
        // call on the next line, so no lifetime concerns - the slice is gone before the storage
        // goes away in all cases.
        let listen_socket_as_slice = unsafe {
            slice::from_raw_parts(
                mem::transmute(&listen_socket as *const _),
                mem::size_of::<usize>(),
            )
        };

        // This does some internal updates in the socket. The documentation is a little vague about
        // what this accomplishes but we might as well do it to be right and proper in all ways.
        winsock::to_io_result(unsafe {
            setsockopt(
                *connection_socket,
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                Some(listen_socket_as_slice),
            )
        })
        .unwrap();

        // The new socket is connected and ready! Finally!
        let connection = TcpConnection {
            socket: connection_socket,
        };

        // Call the user-provided function to handle the connection.
        let on_accept_clone = on_accept.clone();
        _ = spawn(on_accept_clone(connection));
    }
}

/// Control surface to operate the TCP server. The lifetime of this is not directly connected to the
/// TCP server. Dropping this will not stop the server - you must explicitly call `stop()` to stop
/// the server, and may call `wait()` to wait for the server to complete its shutdown process.
pub struct TcpServerHandle {}

impl TcpServerHandle {
    /// Stop the server. This will signal the server that it is to stop accepting new connections,
    /// and will start terminating existing connections. The method returns immediately. If you want
    /// to wait for the server to complete its shutdown process, call `wait()` after calling this.
    pub fn stop(&mut self) {
        todo!()
    }

    /// Wait for the server to complete its shutdown process. This will not return until the server
    /// has stopped accepting new connections, and all existing connections have been closed.
    pub async fn wait(&mut self) {
        todo!()
    }
}

#[negative_impl]
impl !Send for TcpServerHandle {}
#[negative_impl]
impl !Sync for TcpServerHandle {}
